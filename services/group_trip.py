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
async def get_group_details(group_id: int):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Get group
    cursor.execute("SELECT * FROM group_trip WHERE id = %s", (group_id,))
    group = cursor.fetchone()

    if not group:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Group not found")

    # Get expenses
    cursor.execute("""
        SELECT id, title, amount, added_by_phone, created_at
        FROM group_expense
        WHERE group_id = %s
        ORDER BY created_at DESC
    """, (group_id,))
    expenses = cursor.fetchall()

    cursor.close()
    conn.close()

    # Fix datetime
    for k, v in group.items():
        if isinstance(v, datetime):
            group[k] = v.isoformat()
    for e in expenses:
        if isinstance(e.get("created_at"), datetime):
            e["created_at"] = e["created_at"].isoformat()

    return {"group": group, "expenses": expenses}


# -----------------------------------------
# 3) GET CURRENT ACTIVE GROUP
# -----------------------------------------
async def get_current_group():
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("""
        SELECT * FROM group_trip
        ORDER BY id DESC
        LIMIT 1
    """)
    group = cursor.fetchone()

    cursor.close()
    conn.close()

    if not group:
        return {"group": None}

    # Fix datetime
    for k, v in group.items():
        if isinstance(v, datetime):
            group[k] = v.isoformat()

    return {"group": group}


# -----------------------------------------
# 4) ADD EXPENSE
# -----------------------------------------
async def add_group_expense(request: Request):
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

    if not code:
        raise HTTPException(status_code=400, detail="Access code required")

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("SELECT * FROM group_trip WHERE access_code = %s", (code,))
    group = cursor.fetchone()

    cursor.close()
    conn.close()

    if not group:
        raise HTTPException(status_code=404, detail="Invalid access code")

    for k, v in group.items():
        if isinstance(v, datetime):
            group[k] = v.isoformat()

    return {"success": True, "group": group}
