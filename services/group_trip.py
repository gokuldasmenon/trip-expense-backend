# group_trip.py
# -----------------------------------------
# Group Trip Backend Module for FastAPI
# -----------------------------------------
#
# Every function here now takes an explicit `current_user: dict` (the
# caller identity resolved by auth.get_current_user in main.py's route
# wrappers) instead of trusting a client-supplied user_id/created_by field
# in the request body for AUTHORIZATION purposes. A body field is still
# read where it names a *different* resource (e.g. group_id, expense id),
# never to establish who the caller is.

from fastapi import HTTPException, Request
from datetime import datetime
import psycopg2.extras
import random
import string
import psycopg2
from database import get_connection
from auth import require_group_access, require_group_creator, group_id_for_group_expense


# -----------------------------------------
# Utility: Generate Access Code
# -----------------------------------------
def generate_access_code(length=8):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))


# -----------------------------------------
# 1) CREATE GROUP
# -----------------------------------------
async def group_create(request: Request, current_user: dict):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    name = data.get("name")
    participants = data.get("participants", 1)
    initial_fund = data.get("initial_fund", 0)

    if not name:
        raise HTTPException(status_code=400, detail="Group name required")

    # The creator is always the authenticated caller — a client-supplied
    # created_by is ignored so a group can't be created "owned by" someone
    # else.
    created_by = current_user["id"]

    access_code = generate_access_code()

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("""
        INSERT INTO group_trip (name, participants, initial_fund, current_balance, access_code, created_by)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id, name, participants, initial_fund, current_balance, access_code, created_by, start_date
    """, (name, participants, initial_fund, initial_fund, access_code, created_by))

    group = cursor.fetchone()
    conn.commit()
    cursor.close()
    conn.close()

    # ISO datetime
    for k, v in group.items():
        if isinstance(v, datetime):
            group[k] = v.isoformat()

    return {"success": True, "group": group}


# -----------------------------------------
# 2) GET GROUP DETAILS (group + expenses)
# -----------------------------------------
async def group_get_details(group_id: int, current_user: dict):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # fetch group
    cursor.execute("SELECT * FROM group_trip WHERE id = %s", (group_id,))
    group = cursor.fetchone()
    if not group:
        cursor.close(); conn.close()
        raise HTTPException(status_code=404, detail="Group not found")

    # permission: creator OR participant
    user_id = current_user["id"]
    allowed = False
    if int(group['created_by']) == int(user_id):
        allowed = True
    else:
        cursor.execute("""
            SELECT 1 FROM group_participants
            WHERE group_id = %s AND user_id = %s
            LIMIT 1
        """, (group_id, user_id))
        if cursor.fetchone():
            allowed = True

    if not allowed:
        cursor.close(); conn.close()
        raise HTTPException(status_code=403, detail="Not allowed to view this group")

    # fetch expenses (same as before)
    cursor.execute("""SELECT id, title, amount, added_by_phone, created_at FROM group_expense
                      WHERE group_id = %s ORDER BY created_at DESC""", (group_id,))
    expenses = cursor.fetchall()

    cursor.close(); conn.close()
    # isoformat fixes...
    ...
    # ---- Calculate spending grouped by user phone ----
    spending_map = {}

    for e in expenses:
        phone = e.get("added_by_phone") or "Unknown"
        amount = float(e.get("amount") or 0)

        if phone not in spending_map:
            spending_map[phone] = 0
        spending_map[phone] += amount

    # Convert to list for Flutter
    spending_list = [{"phone": k, "amount": v} for k, v in spending_map.items()]

    return {
        "group": group,
        "expenses": expenses,
        "spending": spending_list
    }



# -----------------------------------------
# 3) GET ALL GROUPS FOR THE CALLER
# -----------------------------------------
async def group_get_all(current_user: dict):
    user_id = current_user["id"]

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Fetch all groups where the caller is creator OR participant
    cursor.execute("""
        SELECT DISTINCT g.*
        FROM group_trip g
        LEFT JOIN group_participants p
             ON g.id = p.group_id
        WHERE g.created_by = %s
           OR p.user_id = %s
        ORDER BY g.id DESC
    """, (user_id, user_id))

    groups = cursor.fetchall()
    cursor.close()
    conn.close()

    # Convert datetime → isoformat
    for g in groups:
        for k, v in g.items():
            if isinstance(v, datetime):
                g[k] = v.isoformat()

    return {"groups": groups}



# -----------------------------------------
# 4) ADD EXPENSE (any group member)
# -----------------------------------------
async def group_add_expense(request: Request, current_user: dict):
    try:
        data = await request.json()
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    group_id = data.get("group_id")
    title = data.get("title")
    amount = data.get("amount")
    phone = data.get("added_by_phone")

    if not group_id or not title or not amount or not phone:
        raise HTTPException(status_code=400, detail="Missing required fields")

    require_group_access(group_id, current_user)

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Insert expense
    cursor.execute("""
        INSERT INTO group_expense (group_id, title, amount, added_by_phone)
        VALUES (%s, %s, %s, %s)
        RETURNING id, group_id, title, amount, added_by_phone, created_at
    """, (group_id, title, amount, phone))

    expense = cursor.fetchone()

    # Update group fund
    cursor.execute("""
        UPDATE group_trip
        SET current_balance = current_balance - %s
        WHERE id = %s
    """, (amount, group_id))

    conn.commit()
    cursor.close()
    conn.close()

    if isinstance(expense.get("created_at"), datetime):
        expense["created_at"] = expense["created_at"].isoformat()

    return {"success": True, "expense": expense}


# -----------------------------------------
# 5) DELETE EXPENSE (any group member)
# -----------------------------------------
async def group_delete_expense(request: Request, current_user: dict):
    data = await request.json()
    expense_id = data.get("id")

    if not expense_id:
        raise HTTPException(status_code=400, detail="Expense id required")

    # Resolve the owning group and check membership before touching anything.
    group_id = group_id_for_group_expense(expense_id)
    require_group_access(group_id, current_user)

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Delete & return deleted row
    cursor.execute("""
        WITH deleted AS (
            DELETE FROM group_expense
            WHERE id = %s
            RETURNING id, group_id, amount
        )
        UPDATE group_trip
        SET current_balance = current_balance + deleted.amount
        FROM deleted
        WHERE group_trip.id = deleted.group_id
        RETURNING deleted.id
    """, (expense_id,))

    result = cursor.fetchone()
    conn.commit()
    cursor.close()
    conn.close()

    if not result:
        raise HTTPException(status_code=404, detail="Expense not found")

    return {"success": True}


# -----------------------------------------
# 6) UPDATE PARTICIPANTS (creator only)
# -----------------------------------------
async def group_update_participants(request: Request, current_user: dict):
    data = await request.json()
    group_id = data.get("group_id")
    participants = data.get("participants")

    if not group_id or participants is None:
        raise HTTPException(status_code=400, detail="Missing fields")

    # The acting user is always the authenticated caller.
    user_id = current_user["id"]
    require_group_creator(group_id, current_user)

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("""
        UPDATE group_trip
        SET participants = %s
        WHERE id = %s AND created_by = %s
        RETURNING *
    """, (participants, group_id, user_id))

    updated = cursor.fetchone()
    conn.commit()
    cursor.close()
    conn.close()

    if not updated:
        raise HTTPException(status_code=403, detail="Only creator can update participants count")

    # Fix datetime
    for k, v in updated.items():
        if isinstance(v, datetime):
            updated[k] = v.isoformat()

    return {"success": True, "group": updated}


# -----------------------------------------
# 7) JOIN GROUP BY ACCESS CODE
# -----------------------------------------
async def group_join(request: Request, current_user: dict):
    data = await request.json()
    code = data.get("code")

    if not code:
        raise HTTPException(status_code=400, detail="Access code required")

    # The joining user is always the authenticated caller.
    user_id = current_user["id"]

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("SELECT * FROM group_trip WHERE access_code = %s", (code,))
    group = cursor.fetchone()
    if not group:
        cursor.close(); conn.close()
        raise HTTPException(status_code=404, detail="Invalid access code")

    # Insert into participants (ignore duplicate)
    try:
        cursor.execute("""
            INSERT INTO group_participants (group_id, user_id)
            VALUES (%s, %s)
            ON CONFLICT (group_id, user_id) DO NOTHING
            RETURNING id
        """, (group['id'], user_id))
        conn.commit()
    except Exception:
        conn.rollback()

    # return group details (optional: filtered)
    for k, v in group.items():
        if isinstance(v, datetime):
            group[k] = v.isoformat()
    cursor.close(); conn.close()
    return {"success": True, "group": group}

# 8) EDIT EXPENSE (any group member)
async def group_edit_expense(request: Request, current_user: dict):
    data = await request.json()
    expense_id = data.get("id")
    new_title = data.get("title")
    new_amount = data.get("amount")

    if not expense_id or not new_title or new_amount is None:
        raise HTTPException(status_code=400, detail="Missing fields")

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # Fetch old values
        cursor.execute("""
            SELECT group_id, amount FROM group_expense WHERE id = %s
        """, (expense_id,))
        old = cursor.fetchone()

        if not old:
            raise HTTPException(status_code=404, detail="Expense not found")

        group_id = old["group_id"]
        require_group_access(group_id, current_user)

        # Convert types: required because Postgres returns Decimal
        old_amount = float(old["amount"])
        new_amount = float(new_amount)

        diff = new_amount - old_amount

        # Update title & amount
        cursor.execute("""
            UPDATE group_expense
            SET title = %s, amount = %s
            WHERE id = %s
        """, (new_title, new_amount, expense_id))

        # Update group balance (+ decrease by diff)
        cursor.execute("""
            UPDATE group_trip
            SET current_balance = current_balance - %s
            WHERE id = %s
        """, (diff, group_id))

        conn.commit()
        return {"success": True}

    except HTTPException:
        # Auth/validation failures should propagate as real HTTP errors,
        # not get swallowed into a 200 "success: false" response.
        raise
    except Exception as e:
        conn.rollback()
        return {"success": False, "error": str(e)}
    finally:
        cursor.close()
        conn.close()


# -----------------------------------------
# 9) UPDATE INITIAL FUND (creator only)
# -----------------------------------------
async def group_update_initial_fund(request: Request, current_user: dict):
    data = await request.json()
    group_id = data.get("group_id")
    initial_fund = data.get("initial_fund")

    if not group_id or initial_fund is None:
        raise HTTPException(status_code=400, detail="Missing fields")

    # Setting the initial fund is an owner-level trip-lifecycle action.
    require_group_creator(group_id, current_user)

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Update and recalc balance difference
    cursor.execute("""
        UPDATE group_trip
        SET current_balance = current_balance + (%s - initial_fund),
            initial_fund = %s
        WHERE id = %s
        RETURNING *
    """, (initial_fund, initial_fund, group_id))

    group = cursor.fetchone()
    conn.commit()
    cursor.close()
    conn.close()

    return {"success": True, "group": group}


# -----------------------------------------
# 10) EXIT GROUP (any member except the creator)
# -----------------------------------------
async def group_exit(request: Request, current_user: dict):
    data = await request.json()
    group_id = data.get("group_id")

    if not group_id:
        raise HTTPException(status_code=400, detail="Missing fields")

    # The exiting user is always the authenticated caller — any member (not
    # just the creator) may exit their own membership.
    user_id = current_user["id"]

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Prevent creator from exiting
    cursor.execute("SELECT created_by FROM group_trip WHERE id=%s", (group_id,))
    row = cursor.fetchone()

    if not row:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Group not found")

    if row["created_by"] == user_id:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=403, detail="Creator cannot exit the group")

    # Remove participant
    cursor.execute(
        "DELETE FROM group_participants WHERE group_id=%s AND user_id=%s",
        (group_id, user_id),
    )
    conn.commit()
    cursor.close()
    conn.close()

    return {"success": True}


# -----------------------------------------
# 11) DELETE GROUP (creator only)
# -----------------------------------------
async def group_delete(request: Request, current_user: dict):
    data = await request.json()
    group_id = data.get("group_id")

    if not group_id:
        raise HTTPException(status_code=400, detail="Missing fields")

    # The acting user is always the authenticated caller.
    user_id = current_user["id"]

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Check creator
    cursor.execute("SELECT created_by FROM group_trip WHERE id=%s", (group_id,))
    row = cursor.fetchone()

    if not row:
        cursor.close(); conn.close()
        raise HTTPException(status_code=404, detail="Group not found")

    if row["created_by"] != user_id:
        cursor.close(); conn.close()
        raise HTTPException(status_code=403, detail="Only creator can delete group trip")

    # Delete related expenses + participants first
    cursor.execute("DELETE FROM group_expense WHERE group_id=%s", (group_id,))
    cursor.execute("DELETE FROM group_participants WHERE group_id=%s", (group_id,))

    # Delete group
    cursor.execute("DELETE FROM group_trip WHERE id=%s", (group_id,))

    conn.commit()
    cursor.close()
    conn.close()

    return {"success": True}
