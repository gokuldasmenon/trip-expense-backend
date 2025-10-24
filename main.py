import os
import traceback
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import psycopg2, psycopg2.extras, random, string
from datetime import datetime
import time
from services.settlement import calculate_stay_settlement, get_settlement, record_stay_settlement, record_trip_settlement
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

# @app.get("/settlement/{trip_id}")
# def settlement_endpoint(trip_id: int, start_date: str = None, end_date: str = None, record: bool = False):
#     return settlement.get_settlement(trip_id, start_date, end_date, record)


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

# ==========================================
# 🧾 LIST ALL STAY SETTLEMENTS
# ==========================================
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


# ==========================================
# 🧾 GET SINGLE STAY SETTLEMENT DETAILS
# ==========================================
@app.get("/stay_settlement/{settlement_id}")
def get_stay_settlement_detail(settlement_id: int):
    """
    Retrieve details for a specific recorded stay settlement.
    Includes settlement header and each family's contribution/balance.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ✅ Settlement header
    cursor.execute("""
        SELECT s.id, s.trip_id, t.name AS trip_name, s.period_start, s.period_end,
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

    # ✅ Family details
    cursor.execute("""
        SELECT 
            d.family_id,
            f.family_name,
            d.members_count,
            d.total_spent,
            d.due_amount,
            d.balance
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


# ==========================================
# 🏠 RECORD A STAY SETTLEMENT
# ==========================================
@app.post("/record_stay_settlement/{trip_id}")
def record_stay_settlement_endpoint(trip_id: int):
    """
    Computes and records a stay settlement for the given trip.
    Creates entries in stay_settlements and stay_settlement_details.
    """
    try:
        print(f"🟢 Starting stay settlement recording for trip_id={trip_id}")
        result = calculate_stay_settlement(trip_id)
        print(f"✅ Calculation complete: total_expense={result['total_expense']}, per_head_cost={result['per_head_cost']}")
        settlement_id = record_stay_settlement(trip_id, result)
        print(f"💾 Recorded stay settlement with ID {settlement_id}")
        return {
            "message": f"Stay settlement recorded successfully for trip {trip_id}",
            "settlement_id": settlement_id
        }
    except Exception as e:
        import traceback
        print("❌ Error while recording stay settlement:", e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to record stay settlement: {e}")

# ==========================================
# 📜 VIEW CARRY-FORWARD HISTORY FOR A TRIP
# ==========================================
# ==========================================
# 📜 VIEW CARRY-FORWARD HISTORY (OPTIONAL FAMILY FILTER)
# ==========================================
from fastapi import Query

@app.get("/stay_carry_forward_log/{trip_id}")
def get_carry_forward_log(trip_id: int, family_id: int = Query(None)):
    """
    Retrieves carry-forward log(s) for a Stay trip.
    If family_id is provided, returns logs only for that family.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        if family_id:
            print(f"📘 Fetching carry-forward logs for trip {trip_id}, family {family_id}")
            cursor.execute("""
                SELECT 
                    l.id,
                    l.trip_id,
                    l.previous_settlement_id,
                    l.new_settlement_id,
                    l.family_id,
                    f.family_name,
                    l.previous_balance,
                    l.new_balance,
                    l.delta,
                    l.created_at
                FROM stay_carry_forward_log l
                JOIN family_details f ON l.family_id = f.id
                WHERE l.trip_id = %s AND l.family_id = %s
                ORDER BY l.created_at DESC;
            """, (trip_id, family_id))
        else:
            print(f"📘 Fetching all carry-forward logs for trip {trip_id}")
            cursor.execute("""
                SELECT 
                    l.id,
                    l.trip_id,
                    l.previous_settlement_id,
                    l.new_settlement_id,
                    l.family_id,
                    f.family_name,
                    l.previous_balance,
                    l.new_balance,
                    l.delta,
                    l.created_at
                FROM stay_carry_forward_log l
                JOIN family_details f ON l.family_id = f.id
                WHERE l.trip_id = %s
                ORDER BY l.created_at DESC;
            """, (trip_id,))

        records = cursor.fetchall()
        conn.close()

        if not records:
            msg = f"No carry-forward history found for trip {trip_id}"
            if family_id:
                msg += f" and family {family_id}"
            return {"trip_id": trip_id, "family_id": family_id, "message": msg}

        return {
            "trip_id": trip_id,
            "family_id": family_id,
            "total_records": len(records),
            "carry_forward_history": records
        }

    except Exception as e:
        import traceback
        print("❌ Error retrieving carry-forward log:", e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to fetch carry-forward log: {e}")



@app.get("/settlement/{trip_id}")
def unified_settlement_endpoint(
    trip_id: int,
    mode: str = "TRIP",
    period: str = None,
    record: bool = False
):
    """
    Unified settlement endpoint for both TRIP and STAY modes.
    - mode = TRIP or STAY
    - period = optional (e.g., 'monthly' or custom date range)
    - record = if True, records the settlement permanently
    """

    try:
        print(f"🧮 Starting unified settlement computation for trip_id={trip_id}, mode={mode}")

        if mode.upper() == "STAY":
            # =============================
            # 🏠 STAY MODE CALCULATION
            # =============================
            result = calculate_stay_settlement(trip_id)

            result["mode"] = "STAY"
            result["timestamp"] = datetime.utcnow().isoformat()

            # Add carry-forward summary for reporting
            if "families" in result:
                result["carry_forward_total"] = round(
                    sum(f.get("previous_balance", 0.0) for f in result["families"]), 2
                )

            # Compact summary for dashboard view
            result["summary"] = {
                "total_expense": result.get("total_expense", 0.0),
                "total_members": result.get("total_members", 0),
                "per_head_cost": result.get("per_head_cost", 0.0),
                "families_count": len(result.get("families", []))
            }

            # 📝 Optionally record it
            if record:
                settlement_id = record_stay_settlement(trip_id, result)
                result["recorded_settlement_id"] = settlement_id
                result["message"] = f"Stay settlement recorded successfully (ID {settlement_id})"

            return result

        else:
            # =============================
            # 🧳 TRIP MODE CALCULATION
            # =============================
            result = get_settlement(trip_id)

            result["mode"] = "TRIP"
            result["timestamp"] = datetime.utcnow().isoformat()

            result["summary"] = {
                "total_expense": result.get("total_expense", 0.0),
                "total_members": result.get("total_members", 0),
                "per_head_cost": result.get("per_head_cost", 0.0),
                "families_count": len(result.get("families", []))
            }

            if record:
                record_trip_settlement(trip_id, result)
                result["message"] = "Trip settlement recorded successfully"

            return result

    except Exception as e:
        import traceback
        print("❌ Unified settlement failed:", e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Settlement generation failed: {e}")


@app.get("/trip_settlements/{trip_id}")
def list_trip_settlements(trip_id: int):
    """
    List all recorded settlements for a given Trip.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("""
        SELECT id, trip_id, period_start, period_end, total_expense, per_head_cost, created_at
        FROM trip_settlements
        WHERE trip_id = %s
        ORDER BY id DESC
    """, (trip_id,))
    records = cursor.fetchall()

    cursor.close()
    conn.close()

    if not records:
        return {"message": f"No trip settlements found for trip_id {trip_id}"}

    return {"trip_id": trip_id, "settlement_records": records}

@app.get("/trip_settlement/{settlement_id}")
def get_trip_settlement_detail(settlement_id: int):
    """
    Retrieve details for a specific recorded trip settlement.
    Includes each family's contribution and balance.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ✅ Settlement header
    cursor.execute("""
        SELECT 
            s.id, s.trip_id, t.name AS trip_name,
            s.period_start, s.period_end, 
            s.total_expense, s.per_head_cost, s.created_at
        FROM trip_settlements s
        JOIN trips t ON s.trip_id = t.id
        WHERE s.id = %s
    """, (settlement_id,))
    settlement = cursor.fetchone()

    if not settlement:
        cursor.close()
        conn.close()
        return {"error": f"Trip settlement record {settlement_id} not found"}

    # ✅ Family-level settlement details
    cursor.execute("""
        SELECT 
            d.family_id, 
            f.family_name, 
            d.members_count, 
            d.total_spent, 
            d.due_amount, 
            d.balance
        FROM trip_settlement_details d
        JOIN family_details f ON d.family_id = f.id
        WHERE d.settlement_id = %s
        ORDER BY f.family_name ASC
    """, (settlement_id,))
    details = cursor.fetchall()

    cursor.close()
    conn.close()

    settlement["details"] = details
    return settlement

