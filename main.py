from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import psycopg2, psycopg2.extras, random, string
from datetime import datetime
from typing import Optional

# Local imports
from database import get_connection, initialize_database
from models import (
    TripIn, FamilyIn, ExpenseIn,
    FamilyUpdate, ExpenseUpdate, AdvanceModel, UserIn
)
from services import trips, families, expenses, advances, settlement

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
@app.post("/login_user")
async def login_user(request: Request):
    """
    Login by phone or email.
    If user not found, auto-register them (no manual registration needed).
    """
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    phone = data.get("phone")
    email = data.get("email")
    name = data.get("name") or "User"

    if not phone and not email:
        raise HTTPException(status_code=400, detail="Provide either phone or email")

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ✅ Only check by provided field
    if phone:
        cursor.execute("""
            SELECT id, name, phone, email, created_at
            FROM users
            WHERE phone = %s
        """, (phone,))
    else:
        cursor.execute("""
            SELECT id, name, phone, email, created_at
            FROM users
            WHERE email = %s
        """, (email,))

    user = cursor.fetchone()

    # 🟩 Auto-register if not found
    if not user:
        cursor.execute("""
            INSERT INTO users (name, phone, email)
            VALUES (%s, %s, %s)
            RETURNING id, name, phone, email, created_at
        """, (name, phone, email))
        user = cursor.fetchone()
        conn.commit()

    cursor.close()
    conn.close()

    # Safe datetime serialization
    for k, v in user.items():
        if isinstance(v, datetime):
            user[k] = v.isoformat()

    return {"message": "✅ Login successful", "user": user}


@app.post("/register_user")
def register_user(user: dict):
    """Register a new user or return if exists (by valid phone/email only)."""
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        name = user.get("name", "User")
        phone = user.get("phone")
        email = user.get("email")

        if not phone and not email:
            raise HTTPException(status_code=400, detail="Phone or Email is required.")

        # 🧹 Normalize blanks to None
        phone = phone.strip() if phone and phone.strip() else None
        email = email.strip() if email and email.strip() else None

        # 🔍 Build query dynamically (ignore NULL/blank values)
        if phone and email:
            cursor.execute("""
                SELECT id, name, phone, email, created_at
                FROM users
                WHERE phone = %s OR email = %s
            """, (phone, email))
        elif phone:
            cursor.execute("""
                SELECT id, name, phone, email, created_at
                FROM users
                WHERE phone = %s
            """, (phone,))
        elif email:
            cursor.execute("""
                SELECT id, name, phone, email, created_at
                FROM users
                WHERE email = %s
            """, (email,))
        else:
            raise HTTPException(status_code=400, detail="Provide valid phone or email")

        existing = cursor.fetchone()

        # 🟢 Create new user if not found
        if not existing:
            cursor.execute("""
                INSERT INTO users (name, phone, email)
                VALUES (%s, %s, %s)
                RETURNING id, name, phone, email, created_at
            """, (name, phone, email))
            existing = cursor.fetchone()
            conn.commit()
            msg = "✅ User registered successfully"
        else:
            msg = "User already registered"

        return {"message": msg, "user": existing}

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()


# ================================================
# 🧳 TRIPS
# ================================================
def generate_access_code(length=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))


@app.post("/add_trip")
def add_trip(trip: TripIn):
    """
    Creates a new trip and auto-registers the owner in trip_members.
    """
    conn = get_connection()
    cursor = conn.cursor()

    access_code = generate_access_code()

    cursor.execute("""
        INSERT INTO trips (name, start_date, trip_type, access_code, owner_name, owner_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id, name, start_date, trip_type, access_code, owner_name, owner_id, created_at
    """, (trip.name, trip.start_date, trip.trip_type, access_code, trip.owner_name, trip.owner_id))

    new_trip = cursor.fetchone()

    # 🟩 Auto-register owner as member
    cursor.execute("""
        INSERT INTO trip_members (trip_id, user_id, role)
        VALUES (%s, %s, 'owner')
        ON CONFLICT DO NOTHING
    """, (new_trip[0], trip.owner_id))

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
            "created_at": new_trip[7],
        },
    }


@app.post("/join_trip/{access_code}")
def join_trip(access_code: str, user_id: int):
    """
    Join a trip using access code + user_id.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # ✅ Ensure user exists
        cursor.execute("SELECT id, name FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail=f"User {user_id} not found")

        # ✅ Find trip
        cursor.execute("""
            SELECT id, name, start_date, trip_type, access_code, owner_id
            FROM trips
            WHERE access_code = %s
        """, (access_code,))
        trip = cursor.fetchone()
        if not trip:
            raise HTTPException(status_code=404, detail="Invalid access code")

        role = "owner" if user_id == trip["owner_id"] else "member"

        # ✅ Insert membership
        cursor.execute("""
            INSERT INTO trip_members (trip_id, user_id, role)
            VALUES (%s, %s, %s)
            ON CONFLICT (trip_id, user_id) DO NOTHING
        """, (trip["id"], user_id, role))

        conn.commit()
        print(f"DEBUG: Joined trip_id={trip['id']} user_id={user_id} role={role}")

        return {"message": "Joined trip successfully", "trip": trip, "role": role}

    except Exception as e:
        conn.rollback()
        print(f"❌ ERROR in join_trip: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.get("/trips/{user_id}")
def get_trips_for_user(user_id: int):
    """
    Returns only ACTIVE trips:
    - own_trips → created by the user
    - joined_trips → joined by the user (but not owned)
    Archived trips are excluded.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # 👑 Owned trips (ACTIVE only)
    cursor.execute("""
        SELECT id, name, start_date, trip_type, access_code, owner_name, created_at
        FROM trips
        WHERE owner_id = %s AND (status IS NULL OR status != 'ARCHIVED')
        ORDER BY id DESC
    """, (user_id,))
    own_trips = cursor.fetchall()

    # 🤝 Joined trips (ACTIVE only)
    cursor.execute("""
        SELECT t.id, t.name, t.start_date, t.trip_type, t.access_code, t.owner_name, t.created_at
        FROM trips t
        JOIN trip_members tm ON tm.trip_id = t.id
        WHERE tm.user_id = %s
          AND t.owner_id != %s
          AND (t.status IS NULL OR t.status != 'ARCHIVED')
        ORDER BY t.id DESC
    """, (user_id, user_id))
    joined_trips = cursor.fetchall()

    cursor.close()
    conn.close()

    return {
        "own_trips": own_trips,
        "joined_trips": joined_trips
    }



@app.get("/debug_members")
def debug_trip_members():
    """Show all trip-member relations for debugging."""
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("""
        SELECT tm.id, tm.trip_id, t.name AS trip_name,
               tm.user_id, u.name AS user_name,
               tm.role, tm.joined_at
        FROM trip_members tm
        LEFT JOIN trips t ON tm.trip_id = t.id
        LEFT JOIN users u ON tm.user_id = u.id
        ORDER BY tm.id DESC
    """)
    members = cursor.fetchall()
    cursor.close()
    conn.close()

    return {"count": len(members), "memberships": members}


@app.get("/trip/{trip_id}")
def get_trip(trip_id: int):
    """Fetch single trip with owner info."""
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("""
        SELECT t.*, u.name AS owner_name
        FROM trips t
        LEFT JOIN users u ON t.owner_id = u.id
        WHERE t.id = %s
    """, (trip_id,))
    trip = cursor.fetchone()
    cursor.close()
    conn.close()

    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")

    for k, v in trip.items():
        if isinstance(v, datetime):
            trip[k] = v.isoformat()

    return JSONResponse(content=dict(trip))


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

@app.get("/sync_settlement/{trip_id}")
def sync_settlement(trip_id: int):
    """
    Returns settlement in format expected by Flutter.
    Includes timestamp and wraps settlement data inside "data".
    """
    try:
        result = settlement.get_settlement(trip_id)
        return {
            "data": result,
            "last_sync": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Settlement sync failed: {e}")


@app.get("/trip_summary/{trip_id}")
def trip_summary(trip_id: int):
    return settlement.get_trip_summary(trip_id)

@app.put("/trips/archive/{trip_id}")
def archive_trip(trip_id: int):
    return trips.archive_trip(trip_id)

@app.delete("/trips/{trip_id}")
def delete_trip(trip_id: int): 
    return trips.delete_trip(trip_id)
@app.put("/trips/restore/{trip_id}")
def restore_trip_endpoint(trip_id: int):
    return trips.restore_trip(trip_id)

@app.get("/archived_trips")
def get_archived_trips_endpoint():
    return trips.get_archived_trips()