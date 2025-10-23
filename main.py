import os
import traceback
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import psycopg2, psycopg2.extras, random, string
from datetime import datetime
import time

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
IS_DEV = os.environ.get("ENV", "development") == "development"
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """
    Logs:
      ✅ All requests (if in development)
      ⚠️ Only slow (>500ms) or failed ones in production
    """
    start_time = time.time()

    try:
        response = await call_next(request)
    except Exception as e:
        process_time = (time.time() - start_time) * 1000
        print(f"❌ ERROR {request.method} {request.url.path} ({process_time:.2f} ms): {e}")
        raise

    process_time = (time.time() - start_time) * 1000
    status = response.status_code

    # Always log if development or if slow/error
    if IS_DEV or process_time > 500 or status >= 400:
        query = f"?{request.url.query}" if request.url.query else ""
        print(
            f"{'⚠️' if process_time > 500 else '✅'} "
            f"{request.method} {request.url.path}{query} "
            f"→ {status} ({process_time:.2f} ms)"
        )

    return response
# ================================================
# 🏁 STARTUP + HEALTH CHECK
# ================================================
@app.on_event("startup")
def on_startup():
    initialize_database()


@app.get("/")
def home():
    return {"message": "✅ Expense Tracker Backend Running Now"}


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
    Creates a new trip or stay session.
    Automatically assigns owner and mode (TRIP/STAY).
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        access_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

        cursor.execute("""
            INSERT INTO trips (name, start_date, trip_type, mode, billing_cycle, access_code,
                               status, owner_name, owner_id)
            VALUES (%s, %s, %s, %s, %s, %s, 'ACTIVE', %s, %s)
            RETURNING *
        """, (
            trip.name,
            trip.start_date,
            trip.trip_type,
            getattr(trip, 'mode', 'TRIP'),            # default TRIP
            getattr(trip, 'billing_cycle', None),     # optional for STAY
            access_code,
            getattr(trip, 'owner_name', 'User'),
            getattr(trip, 'owner_id', None),
        ))

        new_trip = cursor.fetchone()
        conn.commit()

        # Auto-register owner as member
        cursor.execute("""
            INSERT INTO trip_members (trip_id, user_id, role)
            VALUES (%s, %s, 'owner')
            ON CONFLICT DO NOTHING
        """, (new_trip['id'], trip.owner_id))
        conn.commit()

        return {
            "message": "Session created successfully",
            "trip": new_trip
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Trip creation failed: {e}")
    finally:
        cursor.close()
        conn.close()




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
def get_trips_for_user_endpoint(user_id: int):
    """
    API endpoint: returns all ACTIVE trips (own + joined) for a user.
    Delegates logic to trips.get_trips_for_user() in services/trips.py.
    """
    try:
        result = trips.get_trips_for_user(user_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching trips: {e}")





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
def settlement_endpoint(trip_id: int, start_date: str = None, end_date: str = None, record: bool = False):
    return settlement.get_settlement(trip_id, start_date, end_date, record)


@app.get("/sync_settlement/{trip_id}")
def sync_settlement(trip_id: int):
    """
    Returns settlement in format expected by Flutter.
    Includes timestamp and wraps settlement data inside "data".
    Logs detailed traceback for Render debugging.
    """
    try:
        result = settlement.get_settlement(trip_id)
        return {
            "data": result,
            "last_sync": datetime.utcnow().isoformat()
        }
    except Exception as e:
        # Print full traceback to Render logs
        print("❌ ERROR in /sync_settlement endpoint:")
        traceback.print_exc()

        # Return sanitized error message to client
        raise HTTPException(
            status_code=500,
            detail=f"Settlement sync failed: {type(e).__name__}: {e}"
        )


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

# ============================
# 🏠 STAY SETTLEMENT RECORDS
# ============================

@app.get("/stay_settlements/{trip_id}")
def list_stay_settlements(trip_id: int):
    """
    List all recorded settlements for a given Stay trip.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("""
        SELECT id, trip_id, period_start AS start_date, period_end AS end_date,
            total_expense, per_head_cost, created_at
        FROM stay_settlements
        WHERE trip_id = %s
        ORDER BY id DESC
    """, (trip_id,))


    records = cursor.fetchall()

    cursor.close()
    conn.close()

    if not records:
        return {"message": f"No stay settlements found for trip_id {trip_id}"}

    return {"trip_id": trip_id, "settlement_records": records}


@app.get("/stay_settlement/{settlement_id}")
def get_stay_settlement_detail(settlement_id: int):
    """
    Retrieve details for a specific recorded stay settlement.
    Includes each family's contribution and balance.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ✅ Settlement header
    cursor.execute("""
        SELECT s.id, s.trip_id, t.name AS trip_name,
            s.period_start AS start_date, s.period_end AS end_date,
            s.total_expense, s.per_head_cost, s.created_at
        FROM stay_settlements s
        JOIN trips t ON s.trip_id = t.id
        WHERE s.id = %s
    """, (settlement_id,))


    settlement = cursor.fetchone()

    if not settlement:
        cursor.close()
        conn.close()
        return {"error": f"Settlement record {settlement_id} not found"}

    # ✅ Settlement details (family-wise)
    cursor.execute("""
        SELECT d.family_id, f.family_name, d.amount_spent, d.balance
        FROM stay_settlement_details d
        JOIN family_details f ON d.family_id = f.id
        WHERE d.settlement_id = %s
        ORDER BY f.family_name ASC
    """, (settlement_id,))
    details = cursor.fetchall()

    cursor.close()
    conn.close()

    settlement["details"] = details
    return settlement
