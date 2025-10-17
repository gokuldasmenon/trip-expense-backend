from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import psycopg2, psycopg2.extras, random, string
from datetime import datetime
from fastapi.responses import JSONResponse
import psycopg2.extras
from fastapi import Request
# Local imports
from database import get_connection, initialize_database
from models import (
    TripIn, FamilyIn, ExpenseIn,
    FamilyUpdate, ExpenseUpdate, AdvanceModel, UserIn
)
from services import trips, families, expenses, advances, settlement
from datetime import datetime
# --------------------------------------------
app = FastAPI(title="Expense Tracker API")
# --------------------------------------------

# ✅ Enable CORS for Flutter
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================================================
# 🏁 STARTUP + HEALTH CHECK
# ================================================
@app.on_event("startup")
def on_startup():
    initialize_database()


@app.get("/")
def home():
    return {"message": "✅ Expense Tracker Backend Running"}


# ================================================
# 👥 USERS
# ================================================
@app.post("/register_user")
def register_user(user: UserIn):
    """Register user by name, phone, and/or email."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Check if user already exists (by phone or email)
        cursor.execute("""
            SELECT id, name, phone, email, created_at
            FROM users
            WHERE phone = %s OR email = %s
        """, (user.phone, user.email))
        existing = cursor.fetchone()

        if existing:
            return {
                "message": "User already registered",
                "user": {
                    "id": existing[0],
                    "name": existing[1],
                    "phone": existing[2],
                    "email": existing[3],
                    "created_at": existing[4],
                },
            }

        # Otherwise, insert a new record
        cursor.execute("""
            INSERT INTO users (name, phone, email)
            VALUES (%s, %s, %s)
            RETURNING id, name, phone, email, created_at
        """, (user.name, user.phone, user.email))
        result = cursor.fetchone()

        conn.commit()
        return {
            "message": "✅ User registered successfully",
            "user": {
                "id": result[0],
                "name": result[1],
                "phone": result[2],
                "email": result[3],
                "created_at": result[4],
            },
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()



@app.post("/login_user")
def login_user(user: dict):
    """
    User login with phone or email (email optional).
    Requires prior registration in 'users' table.
    """
    phone = user.get("phone")
    email = user.get("email")

    if not phone and not email:
        raise HTTPException(status_code=400, detail="Provide either phone or email to login")

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("""
        SELECT id, name, phone, email, created_at
        FROM users
        WHERE (%s IS NOT NULL AND phone = %s)
           OR (%s IS NOT NULL AND email = %s)
    """, (phone, phone, email, email))

    user_data = cursor.fetchone()
    cursor.close()
    conn.close()

    if not user_data:
        raise HTTPException(status_code=404, detail="User not registered. Please register first.")

    # ✅ Convert any datetime to string (avoid JSON serialization error)
    from datetime import datetime
    for key, value in user_data.items():
        if isinstance(value, datetime):
            user_data[key] = value.isoformat()

    return {
        "message": "✅ Login successful",
        "user": user_data
    }




# ================================================
# 🧳 TRIPS (with owner + access)
# ================================================
def generate_access_code(length=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))


@app.post("/add_trip")
def add_trip(trip: TripIn):
    conn = get_connection()
    cursor = conn.cursor()

    access_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

    cursor.execute("""
        INSERT INTO trips (name, start_date, trip_type, access_code, owner_name, owner_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id, name, start_date, trip_type, access_code, owner_name, owner_id, created_at
    """, (trip.name, trip.start_date, trip.trip_type, access_code, trip.owner_name, trip.owner_id))

    new_trip = cursor.fetchone()
    conn.commit()
    cursor.close()
    conn.close()

    return {
        "message": "Trip created successfully",
        "trip": {
            "id": new_trip[0],
            "name": new_trip[1],
            "start_date": new_trip[2],
            "trip_type": new_trip[3],
            "access_code": new_trip[4],
            "owner_name": new_trip[5],
            "owner_id": new_trip[6],
            "created_at": new_trip[7]
        }
    }



@app.post("/join_trip/{access_code}")
def join_trip(access_code: str, user_id: int, request: Request = None):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # ✅ 1. Get trip by access code
        cursor.execute("""
            SELECT id, name, trip_type, start_date, owner_id, access_code, created_at
            FROM trips
            WHERE access_code = %s
        """, (access_code,))
        trip = cursor.fetchone()

        if not trip:
            raise HTTPException(status_code=404, detail="Invalid access code")

        trip_id = trip["id"]
        owner_id = trip["owner_id"]

        # ✅ 2. Determine role
        role = "owner" if user_id == owner_id else "member"

        print(f"DEBUG: Joining trip_id={trip_id}, user_id={user_id}, role={role}")

        # ✅ 3. Insert membership safely
        cursor.execute("""
            INSERT INTO trip_members (trip_id, user_id, role)
            VALUES (%s, %s, %s)
            ON CONFLICT (trip_id, user_id) DO NOTHING
        """, (trip_id, user_id, role))
        conn.commit()

        # ✅ 4. Fetch joined trip info
        cursor.execute("""
            SELECT t.id, t.name, t.trip_type, t.start_date, t.access_code, 
                   t.owner_id, u.name AS owner_name, t.created_at
            FROM trips t
            LEFT JOIN users u ON t.owner_id = u.id
            WHERE t.id = %s
        """, (trip_id,))
        trip_data = cursor.fetchone()

        if not trip_data:
            raise HTTPException(status_code=404, detail="Trip not found after join")

        print(f"DEBUG: Trip data fetched successfully: {trip_data}")

        cursor.close()
        conn.close()

        # ✅ 5. Return response (safe JSON)
        return {
            "message": "Joined trip successfully",
            "trip": {
                "id": trip_data["id"],
                "name": trip_data["name"],
                "trip_type": trip_data["trip_type"],
                "start_date": str(trip_data["start_date"]),
                "access_code": trip_data["access_code"],
                "owner_id": trip_data["owner_id"],
                "owner_name": trip_data.get("owner_name", "Unknown"),
                "created_at": str(trip_data.get("created_at", datetime.now())),
            },
            "role": role,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ ERROR in /join_trip: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=f"Join trip failed: {str(e)}")

@app.get("/trips/{user_id}")
def get_user_trips(user_id: int):
    """
    Fetch trips created by the user (own trips)
    and trips they have joined via access code.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # ✅ Trips created by the user (owner)
        cursor.execute("""
            SELECT t.*, u.name AS owner_name
            FROM trips t
            LEFT JOIN users u ON t.owner_id = u.id
            WHERE t.owner_id = %s
            ORDER BY t.created_at DESC
        """, (user_id,))
        own_trips = cursor.fetchall()

        # ✅ Trips the user has joined
        cursor.execute("""
            SELECT t.*, u.name AS owner_name
            FROM trips t
            JOIN trip_members tm ON tm.trip_id = t.id
            LEFT JOIN users u ON t.owner_id = u.id
            WHERE tm.user_id = %s
            ORDER BY t.created_at DESC
        """, (user_id,))
        joined_trips = cursor.fetchall()

        cursor.close()
        conn.close()

        # ✅ Convert datetime fields to string
        from datetime import datetime
        for trip_list in (own_trips, joined_trips):
            for trip in trip_list:
                for key, value in trip.items():
                    if isinstance(value, datetime):
                        trip[key] = value.isoformat()

        return {
            "own_trips": own_trips,
            "joined_trips": joined_trips
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.get("/trip/{trip_id}")
def get_trip(trip_id: int):
    """
    Fetch a single trip by ID (joins owner info for better response)
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT t.id, t.name, t.start_date, t.trip_type, t.access_code,
                   t.status, t.owner_id, u.name AS owner_name, t.created_at
            FROM public.trips t
            LEFT JOIN public.users u ON t.owner_id = u.id
            WHERE t.id = %s
        """, (trip_id,))

        trip = cursor.fetchone()
        cursor.close()
        conn.close()

        if not trip:
            raise HTTPException(status_code=404, detail=f"Trip with ID {trip_id} not found")

        # ✅ Convert any datetime fields to strings (ISO format)
        for key, value in trip.items():
            if isinstance(value, datetime):
                trip[key] = value.isoformat()

        return JSONResponse(content=dict(trip), status_code=200)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/trips")
def get_trips():
    return trips.get_all_trips()


@app.get("/archived_trips")
def get_archived_trips():
    return trips.get_archived_trips()


@app.put("/trips/archive/{trip_id}")
def archive_trip(trip_id: int):
    return trips.archive_trip(trip_id)


@app.put("/trips/restore/{trip_id}")
def restore_trip(trip_id: int):
    return trips.restore_trip(trip_id)


@app.delete("/trips/{trip_id}")
def delete_trip(trip_id: int):
    return trips.delete_trip(trip_id)


# ================================================
# 👨‍👩‍👧 FAMILIES / 💰 EXPENSES / 💸 ADVANCES / 📊 REPORTS
# ================================================
@app.post("/add_family")
def add_family(family: FamilyIn):
    return families.add_family(family.trip_id, family.family_name, family.members_count)


@app.get("/families/{trip_id}")
def get_families(trip_id: int):
    return families.get_families(trip_id)


@app.put("/update_family/{family_id}")
def update_family(family_id: int, family: FamilyUpdate):
    return families.update_family(family_id, family.family_name, family.members_count)


@app.delete("/delete_family/{family_id}")
def delete_family(family_id: int):
    return families.delete_family(family_id)


@app.post("/add_expense")
def add_expense(expense: ExpenseIn):
    return expenses.add_expense(expense.trip_id, expense.payer_id, expense.name, expense.amount, expense.date)


@app.get("/get_expenses/{trip_id}")
def get_expenses(trip_id: int):
    return {"expenses": expenses.get_expenses(trip_id)}


@app.put("/update_expense/{expense_id}")
def update_expense(expense_id: int, expense: ExpenseUpdate):
    return expenses.update_expense(expense_id, expense.payer_id, expense.name, expense.amount, expense.date)


@app.delete("/delete_expense/{expense_id}")
def delete_expense(expense_id: int):
    return expenses.delete_expense(expense_id)


@app.post("/add_advance")
def add_advance(advance: AdvanceModel):
    return advances.add_advance(advance.trip_id, advance.payer_family_id, advance.receiver_family_id, advance.amount, advance.date)


@app.get("/advances/{trip_id}")
def get_advances(trip_id: int):
    return advances.get_advances(trip_id)


@app.get("/settlement/{trip_id}")
def get_settlement(trip_id: int):
    return settlement.get_settlement(trip_id)


@app.get("/trip_summary/{trip_id}")
def trip_summary(trip_id: int):
    return settlement.get_trip_summary(trip_id)
