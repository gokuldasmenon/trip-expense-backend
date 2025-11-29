# group_trip.py
# -----------------------------------------
# Group Trip Backend Module for FastAPI
# -----------------------------------------

from fastapi import HTTPException, Request
from datetime import datetime
import psycopg2.extras
import random
import string
import psycopg2
from database import get_connection


# -----------------------------------------
# Utility: Generate Access Code
# -----------------------------------------
def generate_access_code(length=8):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))


# -----------------------------------------
# 1) CREATE GROUP
# -----------------------------------------
async def group_create(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    name = data.get("name")
    participants = data.get("participants", 1)
    initial_fund = data.get("initial_fund", 0)
    created_by = data.get("created_by")

    if not name:
        raise HTTPException(status_code=400, detail="Group name required")

    if not created_by:
        raise HTTPException(status_code=400, detail="created_by (user_id) required")

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
async def group_get_details(group_id: int, user_id: int | None = None):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # fetch group
    cursor.execute("SELECT * FROM group_trip WHERE id = %s", (group_id,))
    group = cursor.fetchone()
    if not group:
        cursor.close(); conn.close()
        raise HTTPException(status_code=404, detail="Group not found")

    # permission: owner OR participant OR allow if user_id is None and group is public? we deny if None
    allowed = False
    if user_id is not None:
        # Check if creator
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
    return {"group": group, "expenses": expenses}



# -----------------------------------------
# 3) GET CURRENT ACTIVE GROUP
# -----------------------------------------
# group_get_current: returns group visible to the specified user (owner or joined)
async def group_get_current(user_id: int | None = None):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Get the latest group first (you may restrict to active/unfinished later)
    cursor.execute("""
        SELECT * FROM group_trip
        ORDER BY id DESC
        LIMIT 1
    """)
    group = cursor.fetchone()

    if not group:
        cursor.close()
        conn.close()
        return {"group": None}

    # If no user_id passed, hide the group (safer)
    if user_id is None:
        cursor.close()
        conn.close()
        return {"group": None}

    # If user is creator => allowed
    if group['created_by'] == user_id:
        # isoformat fix
        for k, v in group.items():
            if isinstance(v, datetime):
                group[k] = v.isoformat()
        cursor.close()
        conn.close()
        return {"group": group}

    # Otherwise check participants table
    cursor.execute("""
        SELECT 1 FROM group_participants
        WHERE group_id = %s AND user_id = %s
        LIMIT 1
    """, (group['id'], user_id))
    joined = cursor.fetchone()

    cursor.close()
    conn.close()

    if joined:
        for k, v in group.items():
            if isinstance(v, datetime):
                group[k] = v.isoformat()
        return {"group": group}

    # Not creator or joined => hide
    return {"group": None}



# -----------------------------------------
# 4) ADD EXPENSE
# -----------------------------------------
async def group_add_expense(request: Request):
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
# 5) DELETE EXPENSE
# -----------------------------------------
async def group_delete_expense(request: Request):
    data = await request.json()
    expense_id = data.get("id")

    if not expense_id:
        raise HTTPException(status_code=400, detail="Expense id required")

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
async def group_update_participants(request: Request):
    data = await request.json()
    group_id = data.get("group_id")
    participants = data.get("participants")
    user_id = data.get("user_id")

    if not group_id or participants is None or not user_id:
        raise HTTPException(status_code=400, detail="Missing fields")

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
async def group_join(request: Request):
    data = await request.json()
    code = data.get("code")
    user_id = data.get("user_id")   # IMPORTANT: caller should pass their user_id

    if not code:
        raise HTTPException(status_code=400, detail="Access code required")
    if not user_id:
        raise HTTPException(status_code=400, detail="user_id required")

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

# 8) EDIT EXPENSE
async def group_edit_expense(request: Request):
    data = await request.json()
    expense_id = data.get("id")
    new_title = data.get("title")
    new_amount = data.get("amount")

    if not expense_id or not new_title or new_amount is None:
        raise HTTPException(status_code=400, detail="Missing fields")

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # 1. Get old amount and group_id
    cursor.execute("""
        SELECT group_id, amount FROM group_expense WHERE id = %s
    """, (expense_id,))
    old = cursor.fetchone()

    if not old:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Expense not found")

    old_amount = old["amount"]
    group_id = old["group_id"]

    # 2. Update expense
    cursor.execute("""
        UPDATE group_expense
        SET title = %s, amount = %s
        WHERE id = %s
    """, (new_title, new_amount, expense_id))

    # 3. Update group balance difference
    diff = new_amount - old_amount
    cursor.execute("""
        UPDATE group_trip
        SET current_balance = current_balance - %s
        WHERE id = %s
    """, (diff, group_id))

    conn.commit()
    cursor.close()
    conn.close()

    return {"success": True}

async def group_update_initial_fund(request: Request):
    data = await request.json()
    group_id = data.get("group_id")
    initial_fund = data.get("initial_fund")

    if not group_id or initial_fund is None:
        raise HTTPException(status_code=400, detail="Missing fields")

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
async def group_exit(request: Request):
    data = await request.json()
    group_id = data.get("group_id")
    user_id = data.get("user_id")

    if not group_id or not user_id:
        raise HTTPException(status_code=400, detail="Missing fields")

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Prevent creator from exiting
    cursor.execute("SELECT created_by FROM group_trip WHERE id=%s", (group_id,))
    row = cursor.fetchone()

    if row and row["created_by"] == user_id:
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

async def group_delete(request: Request):
    data = await request.json()
    group_id = data.get("group_id")
    user_id = data.get("user_id")

    if not group_id or not user_id:
        raise HTTPException(status_code=400, detail="Missing fields")

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
