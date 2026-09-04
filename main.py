import json
import os
import traceback
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
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
from auth import (
    get_current_user,
    get_current_user_ws,
    is_trip_member,
    require_trip_access,
    require_trip_owner,
    require_group_access,
    require_group_creator,
    trip_id_for_family,
    trip_id_for_expense,
    trip_id_for_advance,
    trip_id_for_settlement_txn,
    trip_id_for_stay_settlement,
    trip_id_for_trip_settlement,
    trip_id_for_carry_forward_log,
    group_id_for_group_expense,
    create_access_token,
    generate_and_store_otp,
    send_otp_sms,
    verify_otp as verify_otp_code,
)
from io import BytesIO
import os
import sys
# main.py
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from services.reports import  generate_settlement_pdf, share_pdf_via_whatsapp
from fastapi import WebSocket
from realtime import ws_manager
from fastapi import Body
from services.group_trip import (
    group_create,
    group_get_details,
    group_add_expense,
    group_delete_expense,
    group_update_participants,
    group_join,
    group_edit_expense,
    group_update_initial_fund,
    group_exit,
    group_delete,
    group_get_all
)
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
from fastapi import Response

@app.get("/")
def home():
    return {"message": "✅ Expense Tracker Backend Running Now"}

@app.head("/")
def home_head():
    return Response(status_code=200)

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.on_event("startup")
def on_startup():
    # your initialize_database() as before
    initialize_database()

    # DEBUG: print routes so we see what's actually live
    print("🔎 Registered routes:")
    for r in app.router.routes:
        try:
            print(f"  • {','.join(sorted(r.methods))} {r.path}")
        except Exception:
            print(f"  • {r}")


# ================================================
# 👥 USERS
# ================================================
@app.post("/auth/request_otp")
async def request_otp(request: Request):
    """
    Step 1 of login: caller supplies a phone number, we generate + store an
    OTP and send it via SMS (or log it for local/dev use if no SMS provider
    is configured).
    """
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    phone = data.get("phone")
    if not phone or not isinstance(phone, str) or not phone.strip():
        raise HTTPException(status_code=400, detail="phone is required")
    phone = phone.strip()

    code = generate_and_store_otp(phone)
    sent_via_sms = send_otp_sms(phone, code)

    response = {"message": "OTP sent" if sent_via_sms else "OTP generated (dev mode)"}
    if not sent_via_sms:
        response["debug_otp"] = code
    return response


@app.post("/auth/verify_otp")
async def verify_otp_endpoint(request: Request):
    """
    Step 2 of login: caller supplies phone + the OTP code they received.
    On success, returns a JWT access token to use as
    'Authorization: Bearer <token>' on every subsequent request.
    """
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    phone = data.get("phone")
    code = data.get("code")
    if not phone or not isinstance(phone, str) or not phone.strip():
        raise HTTPException(status_code=400, detail="phone is required")
    if not code or not isinstance(code, str) or not code.strip():
        raise HTTPException(status_code=400, detail="code is required")
    phone = phone.strip()
    code = code.strip()

    # Raises HTTPException on bad/expired/missing OTP.
    verify_otp_code(phone, code)

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cursor.execute("""
            SELECT id, name, phone, email, created_at
            FROM users
            WHERE phone = %s
        """, (phone,))
        user = cursor.fetchone()

        if not user:
            name = data.get("name") or "User"
            cursor.execute("""
                INSERT INTO users (name, phone, email)
                VALUES (%s, %s, %s)
                RETURNING id, name, phone, email, created_at
            """, (name, phone, None))
            user = cursor.fetchone()
            conn.commit()
    finally:
        cursor.close()
        conn.close()

    for k, v in user.items():
        if isinstance(v, datetime):
            user[k] = v.isoformat()

    token = create_access_token(user["id"], user["phone"])

    return {"message": "Login successful", "token": token, "user": user}


@app.post("/login_user")
async def login_user(request: Request):
    """RETIRED — use /auth/request_otp + /auth/verify_otp instead."""
    raise HTTPException(
        status_code=410,
        detail="This endpoint has been retired. Use /auth/request_otp and /auth/verify_otp instead.",
    )


@app.post("/register_user")
def register_user(user: dict):
    """RETIRED — use /auth/request_otp + /auth/verify_otp instead."""
    raise HTTPException(
        status_code=410,
        detail="This endpoint has been retired. Use /auth/request_otp and /auth/verify_otp instead.",
    )

@app.websocket("/ws/trips/{trip_id}")
async def trip_websocket(websocket: WebSocket, trip_id: int):
    user = await get_current_user_ws(websocket)
    if not user:
        await websocket.close(code=4401)
        return
    if not is_trip_member(trip_id, user["id"]):
        await websocket.close(code=4403)
        return

    user_id = user["id"]
    await ws_manager.connect(trip_id, websocket)
    print(f"🔌 WebSocket connected trip={trip_id} user={user_id}")
    try:
        while True:
            # You can receive ping or ignore messages
            _ = await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(trip_id, websocket)
        print(f"🔌 WebSocket disconnected trip={trip_id} user={user_id}")
    except Exception as e:
        ws_manager.disconnect(trip_id, websocket)
        print(f"❌ WebSocket error: {e}")
# ================================================
# 🧳 TRIPS
# ================================================
def generate_access_code(length=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))


@app.post("/add_trip")
def add_trip(trip: TripIn, current_user: dict = Depends(get_current_user)):
    """
    Creates a new trip or stay session.
    Automatically assigns owner and mode (TRIP/STAY).
    The owner is always the authenticated caller — any client-supplied
    owner_id/owner_name is ignored so a trip can't be created "owned by"
    someone else.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        access_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

        owner_id = current_user["id"]
        owner_name = current_user["name"]

        cursor.execute("""
            INSERT INTO trips (name, start_date, trip_type, mode, billing_cycle, access_code,
                               status, owner_name, owner_id)
            VALUES (%s, %s, %s, %s, %s, %s, 'ACTIVE', %s, %s)
            RETURNING *
        """, (
            trip.name,
            trip.start_date,
            trip.trip_type,
            getattr(trip, 'mode', 'TRIP'),
            getattr(trip, 'billing_cycle', None),
            access_code,
            owner_name,
            owner_id,
        ))

        new_trip = cursor.fetchone()
        conn.commit()

        # 👑 Auto-register owner as trip member
        cursor.execute("""
            INSERT INTO trip_members (trip_id, user_id, role)
            VALUES (%s, %s, 'owner')
            ON CONFLICT (trip_id, user_id) DO NOTHING
        """, (new_trip['id'], owner_id))
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





import json

@app.post("/join_trip/{access_code}")
async def join_trip(access_code: str, current_user: dict = Depends(get_current_user)):
    """
    Join a trip using access code. The joining user is always the
    authenticated caller.
    Also notifies all connected members via WebSocket.
    """
    user_id = current_user["id"]
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # ✅ Ensure user exists
        cursor.execute("SELECT id, name, phone FROM users WHERE id = %s", (user_id,))
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

        # role
        role = "owner" if user_id == trip["owner_id"] else "member"

        # ✅ Insert membership (owner/member logic you already fixed)
        if role == "owner":
            cursor.execute("""
                INSERT INTO trip_members (trip_id, user_id, role)
                VALUES (%s, %s, 'owner')
                ON CONFLICT (trip_id, user_id)
                DO UPDATE SET role = 'owner'
                WHERE trip_members.role != 'owner'
            """, (trip["id"], user_id))
        else:
            cursor.execute("""
                INSERT INTO trip_members (trip_id, user_id, role)
                VALUES (%s, %s, 'member')
                ON CONFLICT (trip_id, user_id) DO NOTHING
            """, (trip["id"], user_id))

        conn.commit()

        print(f"DEBUG: Joined trip_id={trip['id']} user_id={user_id} role={role}")

        # 🔔 Broadcast "member_joined" event (ignore errors)
        try:
            event = {
                "type": "member_joined",
                "trip_id": trip["id"],
                "user_id": user["id"],
                "name": user["name"],
                "phone": user.get("phone"),
                "role": role,
            }
            await ws_manager.broadcast_to_trip(trip["id"], json.dumps(event))
        except Exception as be:
            print(f"⚠️ WebSocket broadcast failed: {be}")

        return {"message": "Joined trip successfully", "trip": trip, "role": role}

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        print(f"❌ ERROR in join_trip: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()



@app.post("/exit_trip/{trip_id}")
def exit_trip(trip_id: int, current_user: dict = Depends(get_current_user)):
    """
    Exit a trip. The exiting user is always the authenticated caller.
    User cannot exit if they are the owner.
    """
    user_id = current_user["id"]
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # 🔍 Check user exists
        cursor.execute("SELECT id, name FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail=f"User {user_id} not found")

        # 🔍 Verify membership + get role
        cursor.execute("""
            SELECT role FROM trip_members
            WHERE trip_id = %s AND user_id = %s
        """, (trip_id, user_id))
        membership = cursor.fetchone()

        if not membership:
            raise HTTPException(status_code=400, detail="User not part of this trip")

        # 🚫 Owners cannot exit (must archive/delete)
        if membership["role"] == "owner":
            raise HTTPException(status_code=403, detail="Owner cannot exit the trip")

        # ❌ Remove membership
        cursor.execute("""
            DELETE FROM trip_members
            WHERE trip_id = %s AND user_id = %s
        """, (trip_id, user_id))

        conn.commit()
        print(f"DEBUG: User {user_id} exited trip {trip_id}")

        return {"success": True, "message": "Exited the trip successfully"}

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        print(f"❌ ERROR in exit_trip: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
@app.post("/remove_member/{trip_id}")
def remove_member(trip_id: int, member_id: int, current_user: dict = Depends(get_current_user)):
    """
    Owner removes another participant from the trip. The acting owner is
    always the authenticated caller.
    """
    require_trip_owner(trip_id, current_user)
    owner_id = current_user["id"]
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # 🔍 Validate owner
        cursor.execute("""
            SELECT role FROM trip_members
            WHERE trip_id = %s AND user_id = %s
        """, (trip_id, owner_id))
        owner = cursor.fetchone()

        if not owner or owner["role"] != "owner":
            raise HTTPException(status_code=403, detail="Only owner can remove participants")

        # 🚫 Owner cannot remove themselves
        if owner_id == member_id:
            raise HTTPException(status_code=400, detail="Owner cannot remove themselves")

        # 🔍 Check member
        cursor.execute("""
            SELECT role FROM trip_members
            WHERE trip_id = %s AND user_id = %s
        """, (trip_id, member_id))
        member = cursor.fetchone()

        if not member:
            raise HTTPException(status_code=404, detail="Member not found in this trip")

        # ❌ Remove member
        cursor.execute("""
            DELETE FROM trip_members
            WHERE trip_id = %s AND user_id = %s
        """, (trip_id, member_id))
        conn.commit()

        return {"success": True, "message": "Member removed successfully"}

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        print(f"❌ ERROR in remove_member: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
@app.get("/trip_members/{trip_id}")
def get_trip_members(trip_id: int, current_user: dict = Depends(get_current_user)):
    """
    Get all members of a trip with their role.
    """
    require_trip_access(trip_id, current_user)
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # 🔍 Validate trip exists
        cursor.execute("SELECT id, name FROM trips WHERE id = %s", (trip_id,))
        trip = cursor.fetchone()
        if not trip:
            raise HTTPException(status_code=404, detail="Trip not found")

        # 👥 Get members
        cursor.execute("""
            SELECT tm.user_id AS id, u.name, u.phone AS mobile, tm.role
            FROM trip_members tm
            JOIN users u ON u.id = tm.user_id
            WHERE tm.trip_id = %s
            ORDER BY tm.role DESC, u.name ASC
        """, (trip_id,))

        members = cursor.fetchall()

        return {"trip_id": trip_id, "members": members}

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ ERROR in get_trip_members: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.get("/trips/{user_id}")
def get_trips_for_user_endpoint(user_id: int, current_user: dict = Depends(get_current_user)):
    """
    API endpoint: returns all ACTIVE trips (own + joined) for a user.
    Delegates logic to trips.get_trips_for_user() in services/trips.py.
    NOTE: the path `user_id` is kept for URL compatibility but is IGNORED
    for data access — we always fetch trips for the authenticated caller,
    so you can't list another user's trips by guessing their id.
    """
    try:
        result = trips.get_trips_for_user(current_user["id"])
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching trips: {e}")



@app.get("/trip/{trip_id}")
def get_trip(trip_id: int, current_user: dict = Depends(get_current_user)):
    """Fetch single trip with owner info."""
    require_trip_access(trip_id, current_user)
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
def add_family(family: FamilyIn, current_user: dict = Depends(get_current_user)):
    require_trip_access(family.trip_id, current_user)
    return families.add_family(family.trip_id, family.family_name, family.members_count)


@app.get("/families/{trip_id}")
def get_families(trip_id: int, current_user: dict = Depends(get_current_user)):
    require_trip_access(trip_id, current_user)
    return families.get_families(trip_id)


@app.put("/update_family/{family_id}")
def update_family(family_id: int, family: FamilyUpdate, current_user: dict = Depends(get_current_user)):
    require_trip_access(trip_id_for_family(family_id), current_user)
    return families.update_family(family_id, family.family_name, family.members_count)


@app.delete("/delete_family/{family_id}")
def delete_family(family_id: int, current_user: dict = Depends(get_current_user)):
    require_trip_access(trip_id_for_family(family_id), current_user)
    return families.delete_family(family_id)


@app.post("/add_expense")
def add_expense(expense: ExpenseIn, current_user: dict = Depends(get_current_user)):
    require_trip_access(expense.trip_id, current_user)
    return expenses.add_expense(expense.trip_id, expense.payer_id, expense.name, expense.amount, expense.date)


@app.get("/get_expenses/{trip_id}")
def get_expenses(trip_id: int, current_user: dict = Depends(get_current_user)):
    require_trip_access(trip_id, current_user)
    return {"expenses": expenses.get_expenses(trip_id)}


@app.put("/update_expense/{expense_id}")
def update_expense(expense_id: int, expense: ExpenseUpdate, current_user: dict = Depends(get_current_user)):
    require_trip_access(trip_id_for_expense(expense_id), current_user)
    return expenses.update_expense(expense_id, expense.payer_id, expense.name, expense.amount, expense.date)


@app.delete("/delete_expense/{expense_id}")
def delete_expense(expense_id: int, current_user: dict = Depends(get_current_user)):
    require_trip_access(trip_id_for_expense(expense_id), current_user)
    return expenses.delete_expense(expense_id)

@app.post("/archive_expenses/{trip_id}/{settlement_id}")
def archive_expenses(trip_id: int, settlement_id: int, current_user: dict = Depends(get_current_user)):
    require_trip_access(trip_id, current_user)
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO expenses_archive (
                trip_id, payer_family_id, amount, date, particulars,
                created_by, updated_by, created_at, updated_at,
                settlement_id, archived_at
            )
            SELECT trip_id, payer_family_id, amount, date, particulars,
                   created_by, updated_by, created_at, updated_at,
                   %s, NOW()
            FROM expenses
            WHERE trip_id = %s;
            """,
            (settlement_id, trip_id),
        )

        cursor.execute(
            "DELETE FROM expenses WHERE trip_id = %s;", (trip_id,)
        )

        conn.commit()
        return {"status": "success", "message": "Expenses archived and cleared."}

    except Exception as e:
        conn.rollback()
        return {"status": "error", "message": str(e)}

    finally:
        conn.close()

@app.get("/expenses-archived/{trip_id}")
def get_archived_expenses(trip_id: int, current_user: dict = Depends(get_current_user)):
    require_trip_access(trip_id, current_user)
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("""
            SELECT 
                ea.id,
                ea.trip_id,
                ea.amount,
                ea.date,
                ea.particulars AS expense_name,
                fd.family_name AS payer,
                ea.created_at
            FROM expenses_archive ea
            LEFT JOIN family_details fd 
                ON ea.payer_family_id = fd.id
            WHERE ea.trip_id = %s
            ORDER BY ea.date DESC NULLS LAST, ea.id DESC
        """, (trip_id,))
        return {"expenses": cur.fetchall()}
    finally:
        cur.close()
        conn.close()


@app.post("/add_advance")
def add_advance(advance: AdvanceModel, current_user: dict = Depends(get_current_user)):
    require_trip_access(advance.trip_id, current_user)
    return advances.add_advance(advance.trip_id, advance.payer_family_id, advance.receiver_family_id, advance.amount, advance.date)


@app.get("/advances/{trip_id}")
def get_advances(trip_id: int, current_user: dict = Depends(get_current_user)):
    require_trip_access(trip_id, current_user)
    return advances.get_advances(trip_id)
@app.get("/archived_advances/{trip_id}")
def get_archived_advances(trip_id: int, current_user: dict = Depends(get_current_user)):
    require_trip_access(trip_id, current_user)
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("""
        SELECT aa.id, aa.trip_id,
               aa.payer_family_id, f1.family_name AS payer_name,
               aa.receiver_family_id, f2.family_name AS receiver_name,
               aa.amount, aa.date, aa.archived_at
        FROM advances_archive aa
        LEFT JOIN family_details f1 ON aa.payer_family_id = f1.id
        LEFT JOIN family_details f2 ON aa.receiver_family_id = f2.id
        WHERE aa.trip_id = %s
        ORDER BY aa.archived_at DESC, aa.id DESC;
    """, (trip_id,))

    out = cursor.fetchall()
    cursor.close()
    conn.close()
    return out

@app.delete("/advance/{advance_id}")
def delete_advance(advance_id: int, current_user: dict = Depends(get_current_user)):
    require_trip_access(trip_id_for_advance(advance_id), current_user)
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM advances WHERE id = %s RETURNING id;", (advance_id,))
        deleted = cursor.fetchone()
        if not deleted:
            raise HTTPException(status_code=404, detail="Advance not found")

        conn.commit()
        return {"message": "Advance deleted successfully", "id": advance_id}

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to delete advance: {str(e)}")

    finally:
        cursor.close()
        conn.close()

# @app.get("/settlement/{trip_id}")
# def settlement_endpoint(trip_id: int, start_date: str = None, end_date: str = None, record: bool = False):
#     return settlement.get_settlement(trip_id, start_date, end_date, record)




@app.put("/advances/{advance_id}")
def update_advance(
    advance_id: int,
    advance: AdvanceModel = Body(...),
    current_user: dict = Depends(get_current_user),
):
    path_trip_id = trip_id_for_advance(advance_id)
    if advance.trip_id != path_trip_id:
        raise HTTPException(status_code=400, detail="trip_id does not match this advance")
    require_trip_access(path_trip_id, current_user)
    return advances.update_advance(
        advance_id,
        advance.trip_id,
        advance.payer_family_id,
        advance.receiver_family_id,
        advance.amount,
        advance.date,
    )





@app.get("/sync_settlement/{trip_id}")
def sync_settlement(trip_id: int, current_user: dict = Depends(get_current_user)):
    """
    Returns settlement in format expected by Flutter  .
    Includes timestamp and wraps settlement data inside "data".
    Logs detailed traceback for Render debugging.
    """
    require_trip_access(trip_id, current_user)
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
def trip_summary(trip_id: int, current_user: dict = Depends(get_current_user)):
    require_trip_access(trip_id, current_user)
    return settlement.get_trip_summary(trip_id)

@app.put("/trips/archive/{trip_id}")
def archive_trip(trip_id: int, current_user: dict = Depends(get_current_user)):
    require_trip_owner(trip_id, current_user)
    return trips.archive_trip(trip_id)

@app.delete("/trips/{trip_id}")
def delete_trip(trip_id: int, current_user: dict = Depends(get_current_user)):
    require_trip_owner(trip_id, current_user)
    return trips.delete_trip(trip_id)
@app.put("/trips/restore/{trip_id}")
def restore_trip_endpoint(trip_id: int, current_user: dict = Depends(get_current_user)):
    require_trip_owner(trip_id, current_user)
    return trips.restore_trip(trip_id)

@app.get("/archived_trips")
def get_archived_trips_endpoint(current_user: dict = Depends(get_current_user)):
    return trips.get_archived_trips(current_user["id"])

# ============================
# 🏠 STAY SETTLEMENT RECORDS
# ============================

# ==========================================
# 🧾 LIST ALL STAY SETTLEMENTS
# ==========================================
@app.get("/stay_settlements/{trip_id}")
def list_stay_settlements(trip_id: int, current_user: dict = Depends(get_current_user)):
    """
    List all recorded settlements for a given Stay trip.
    """
    require_trip_access(trip_id, current_user)
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
def get_stay_settlement_detail(settlement_id: int, current_user: dict = Depends(get_current_user)):
    """
    Retrieve details for a specific recorded stay settlement.
    Includes settlement header and each family's contribution/balance.
    """
    require_trip_access(trip_id_for_stay_settlement(settlement_id), current_user)
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

@app.post("/settlement_transaction")
def add_settlement_transaction(payload: dict, current_user: dict = Depends(get_current_user)):
    """
    Records an actual settlement transaction (money transfer).
    """
    require_trip_access(payload["trip_id"], current_user)
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO settlement_transactions (
            trip_id, from_family_id, to_family_id, amount, remarks
        ) VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
    """, (
        payload["trip_id"],
        payload["from_family_id"],
        payload["to_family_id"],
        payload["amount"],
        payload.get("remarks")
    ))

    transaction_id = cursor.fetchone()[0]
    conn.commit()
    conn.close()

    return {"message": "Transaction recorded successfully", "transaction_id": transaction_id}

@app.get("/settlement_transactions/{trip_id}")
def get_settlement_transactions(trip_id: int, current_user: dict = Depends(get_current_user)):
    """
    Returns all recorded settlement transactions for a given trip.
    """
    require_trip_access(trip_id, current_user)
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("""
        SELECT 
            t.id,
            t.trip_id,
            t.amount,
            t.transaction_date,
            t.remarks,
            f1.family_name AS from_family,
            f2.family_name AS to_family
        FROM settlement_transactions t
        JOIN family_details f1 ON t.from_family_id = f1.id
        JOIN family_details f2 ON t.to_family_id = f2.id
        WHERE t.trip_id = %s
        ORDER BY t.transaction_date DESC;
    """, (trip_id,))

    rows = cursor.fetchall()
    conn.close()
    return {"trip_id": trip_id, "transactions": rows}

@app.get("/settlement_transactions_archive/{trip_id}")
def get_archived_transactions(trip_id: int, current_user: dict = Depends(get_current_user)):
    require_trip_access(trip_id, current_user)
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("""
        SELECT 
            a.id,
            a.amount,
            a.transaction_date,
            a.remarks,
            f1.family_name AS from_family,
            f2.family_name AS to_family,
            a.archived_at
        FROM settlement_transactions_archive a
        JOIN family_details f1 ON a.from_family_id = f1.id
        JOIN family_details f2 ON a.to_family_id = f2.id
        WHERE a.trip_id = %s
        ORDER BY a.archived_at DESC;
    """, (trip_id,))
    rows = cursor.fetchall()
    conn.close()
    return {"trip_id": trip_id, "archived_transactions": rows}

# ==========================================
# 🏠 RECORD A STAY SETTLEMENT
# ==========================================
@app.post("/record_stay_settlement/{trip_id}")
def record_stay_settlement_endpoint(trip_id: int, current_user: dict = Depends(get_current_user)):
    """
    Computes and records a stay settlement for the given trip.
    Creates entries in stay_settlements and stay_settlement_details.
    """
    require_trip_access(trip_id, current_user)
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
# ==============================
# Settlement Transaction Edit/Delete
# ==============================

@app.put("/update_settlement_transaction/{txn_id}")
def update_settlement_transaction(txn_id: int, payload: dict, current_user: dict = Depends(get_current_user)):
    require_trip_access(trip_id_for_settlement_txn(txn_id), current_user)
    conn = get_connection()
    cursor = conn.cursor()

    # Check if transaction belongs to an unfinalized trip
    cursor.execute("""
        SELECT trip_id FROM settlement_transactions WHERE id = %s;
    """, (txn_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return {"error": "Transaction not found."}

    trip_id = row[0]
    cursor.execute("SELECT COUNT(*) FROM stay_settlements WHERE trip_id = %s;", (trip_id,))
    finalized = cursor.fetchone()[0] > 0
    if finalized:
        conn.close()
        return {"error": "Settlement already finalized — editing not allowed."}

    amount = int(round(float(payload.get("amount", 0))))
    remarks = payload.get("remarks", "")
    cursor.execute("""
        UPDATE settlement_transactions
        SET amount = %s, remarks = %s
        WHERE id = %s;
    """, (amount, remarks, txn_id))
    conn.commit()
    conn.close()

    return {"message": "Transaction updated successfully."}


@app.delete("/delete_settlement_transaction/{txn_id}")
def delete_settlement_transaction(txn_id: int, current_user: dict = Depends(get_current_user)):
    require_trip_access(trip_id_for_settlement_txn(txn_id), current_user)
    conn = get_connection()
    cursor = conn.cursor()

    # Verify trip not finalized
    cursor.execute("""
        SELECT trip_id FROM settlement_transactions WHERE id = %s;
    """, (txn_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return {"error": "Transaction not found."}

    trip_id = row[0]
    cursor.execute("SELECT COUNT(*) FROM stay_settlements WHERE trip_id = %s;", (trip_id,))
    finalized = cursor.fetchone()[0] > 0
    if finalized:
        conn.close()
        return {"error": "Settlement already finalized — deletion not allowed."}

    cursor.execute("DELETE FROM settlement_transactions WHERE id = %s;", (txn_id,))
    conn.commit()
    conn.close()

    return {"message": "Transaction deleted successfully."}

# ==========================================
# 📜 VIEW CARRY-FORWARD HISTORY FOR A TRIP
# ==========================================
# ==========================================
# 📜 VIEW CARRY-FORWARD HISTORY (OPTIONAL FAMILY FILTER)
# ==========================================
from fastapi import Query

@app.get("/stay_carry_forward_log/{trip_id}")
def get_carry_forward_log(trip_id: int, family_id: int = Query(None), current_user: dict = Depends(get_current_user)):
    """
    Retrieves carry-forward log(s) for a Stay trip.
    Optionally filters by family_id.
    Includes trip name, stay period, and settlement dates.
    """
    require_trip_access(trip_id, current_user)
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        base_query = """
            SELECT 
                l.id,
                l.trip_id,
                t.name AS trip_name,
                ps.id AS previous_settlement_id,
                ps.period_start AS previous_period_start,
                ps.period_end AS previous_period_end,
                ps.created_at AS previous_settlement_date,
                ns.id AS new_settlement_id,
                ns.period_start AS new_period_start,
                ns.period_end AS new_period_end,
                ns.created_at AS new_settlement_date,
                l.family_id,
                f.family_name,
                l.previous_balance,
                l.new_balance,
                l.delta,
                l.created_at AS log_created_at
            FROM stay_carry_forward_log l
            JOIN family_details f ON l.family_id = f.id
            JOIN trips t ON l.trip_id = t.id
            LEFT JOIN stay_settlements ps ON l.previous_settlement_id = ps.id
            LEFT JOIN stay_settlements ns ON l.new_settlement_id = ns.id
            WHERE l.trip_id = %s
        """

        params = [trip_id]

        if family_id:
            base_query += " AND l.family_id = %s"
            params.append(family_id)

        base_query += " ORDER BY l.created_at DESC;"

        print(f"📘 Fetching carry-forward logs for trip={trip_id}, family={family_id or 'ALL'}")

        cursor.execute(base_query, params)
        records = cursor.fetchall()
        conn.close()

        if not records:
            msg = f"No carry-forward history found for trip {trip_id}"
            if family_id:
                msg += f" and family {family_id}"
            return {"trip_id": trip_id, "family_id": family_id, "message": msg}

        # ✅ Summary metadata
        trip_name = records[0]["trip_name"] if records else None
        summary = {
            "trip_id": trip_id,
            "trip_name": trip_name,
            "family_filter": family_id,
            "total_records": len(records),
            "latest_settlement_date": records[0]["new_settlement_date"] if records else None
        }

        return {
            "summary": summary,
            "carry_forward_history": records
        }

    except Exception as e:
        import traceback
        print("❌ Error retrieving carry-forward log:", e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to fetch carry-forward log: {e}")

@app.get("/stay_carry_forward_logs/{trip_id}")
def list_stay_carry_forward_logs(trip_id: int, current_user: dict = Depends(get_current_user)):
    """
    Returns all carry-forward log entries for a trip,
    enriched with family names and stay period (start → end).
    """
    require_trip_access(trip_id, current_user)
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("""
        SELECT 
            log.id,
            log.trip_id,
            log.previous_settlement_id,
            log.new_settlement_id,
            log.family_id,
            f.family_name,
            log.previous_balance,
            log.new_balance,
            log.delta,
            log.created_at,
            ss.period_start,
            ss.period_end
        FROM stay_carry_forward_log log
        LEFT JOIN family_details f ON log.family_id = f.id
        LEFT JOIN stay_settlements ss ON log.new_settlement_id = ss.id
        WHERE log.trip_id = %s
        ORDER BY log.created_at DESC;
    """, (trip_id,))

    logs = cursor.fetchall()
    conn.close()

    return {"trip_id": trip_id, "logs": logs}


@app.delete("/stay_carry_forward_log/{log_id}")
def delete_stay_carry_forward_log(log_id: int, current_user: dict = Depends(get_current_user)):
    """
    Deletes a single carry-forward log entry.
    """
    require_trip_access(trip_id_for_carry_forward_log(log_id), current_user)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM stay_carry_forward_log WHERE id = %s;", (log_id,))
    conn.commit()
    conn.close()
    return {"message": f"Carry-forward log {log_id} deleted successfully."}

@app.delete("/stay_carry_forward_logs/clear/{trip_id}")
def clear_all_stay_carry_forward_logs(trip_id: int, current_user: dict = Depends(get_current_user)):
    """
    Clears all carry-forward logs for a given trip.
    """
    require_trip_access(trip_id, current_user)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM stay_carry_forward_log WHERE trip_id = %s;", (trip_id,))
    conn.commit()
    conn.close()
    return {"message": f"All carry-forward logs cleared for trip {trip_id}."}

@app.get("/stay_transactions/{settlement_id}")
def get_stay_transactions(settlement_id: int, current_user: dict = Depends(get_current_user)):
    """
    Returns all inter-family transactions recorded for a stay settlement.
    """
    require_trip_access(trip_id_for_stay_settlement(settlement_id), current_user)
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("""
        SELECT t.id, f1.family_name AS payer, f2.family_name AS receiver, t.amount, t.created_at
        FROM stay_transactions t
        JOIN family_details f1 ON t.payer_family_id = f1.id
        JOIN family_details f2 ON t.receiver_family_id = f2.id
        WHERE t.settlement_id = %s
        ORDER BY t.amount DESC;
    """, (settlement_id,))
    transactions = cursor.fetchall()
    conn.close()
    return {"settlement_id": settlement_id, "transactions": transactions}


def _compute_unified_settlement(
    trip_id: int,
    mode: str = "TRIP",
    period: str = None,
    record: bool = False
):
    """
    Plain (non-FastAPI-dependency) helper holding the actual unified
    settlement calculation for both TRIP and STAY modes, so it can be
    called directly from Python (e.g. by settlement_snapshot) as well as
    from the /settlement/{trip_id} route below. Auth/authorization checks
    live on the route wrappers that call this, not here.
    - mode = TRIP or STAY
    - period = optional (e.g., 'monthly' or custom date range)
    - record = if True, records the settlement permanently
    """

    try:
        print(f"🧮 Starting unified settlement computations for trip_id={trip_id}, mode={mode}")

        # =============================
        # 🏠 STAY MODE CALCULATION
        # =============================
        if mode.upper() == "STAY":
            result = calculate_stay_settlement(trip_id)
            result["mode"] = "STAY"
            result["timestamp"] = datetime.utcnow().isoformat()

            # ✅ Ensure adjusted_balance always exists and is numeric
            for fam in result.get("families", []):
                if "adjusted_balance" not in fam:
                    fam["adjusted_balance"] = fam.get("balance", 0.0)
                elif fam["adjusted_balance"] is None:
                    fam["adjusted_balance"] = float(fam.get("balance", 0.0))
                else:
                    fam["adjusted_balance"] = float(fam["adjusted_balance"])

            # 🧾 Carry-forward and summary
            result["carry_forward_total"] = round(
                sum(f.get("previous_balance", 0.0) for f in result["families"]), 2
            )
            result["summary"] = {
                "total_expense": result.get("total_expense", 0.0),
                "total_members": result.get("total_members", 0),
                "per_head_cost": result.get("per_head_cost", 0.0),
                "families_count": len(result.get("families", []))
            }

            # 📝 Optionally record this settlement
            if record:
                settlement_id = record_stay_settlement(trip_id, result)
                result["recorded_settlement_id"] = settlement_id
                result["message"] = f"Stay settlement recorded successfully (ID {settlement_id})"

            print(f"✅ Final STAY result families:")
            for fam in result.get("families", []):
                print(f"  ▶ {fam['family_name']} | Net={fam['balance']} | Adjusted={fam['adjusted_balance']}")

            return result

        # =============================
        # 🧳 TRIP MODE CALCULATION
        # =============================
        else:
            result = get_settlement(trip_id)
            result["mode"] = "TRIP"
            result["timestamp"] = datetime.utcnow().isoformat()

            # ✅ Ensure adjusted_balance exists (same logic)
            for fam in result.get("families", []):
                if "adjusted_balance" not in fam:
                    fam["adjusted_balance"] = fam.get("balance", 0.0)
                elif fam["adjusted_balance"] is None:
                    fam["adjusted_balance"] = float(fam.get("balance", 0.0))
                else:
                    fam["adjusted_balance"] = float(fam["adjusted_balance"])

            # 🧾 Add summary info
            result["summary"] = {
                "total_expense": result.get("total_expense", 0.0),
                "total_members": result.get("total_members", 0),
                "per_head_cost": result.get("per_head_cost", 0.0),
                "families_count": len(result.get("families", []))
            }

            if record:
                record_trip_settlement(trip_id, result)
                result["message"] = "Trip settlement recorded successfully"

            print(f"✅ Final TRIP result families:")
            for fam in result.get("families", []):
                print(f"  ▶ {fam['family_name']} | Net={fam['balance']} | Adjusted={fam['adjusted_balance']}")

            return result

    except Exception as e:
        import traceback
        print("❌ Unified settlement failed:", e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Settlement generation failed: {e}")


@app.get("/settlement/{trip_id}")
def unified_settlement_endpoint(
    trip_id: int,
    mode: str = "TRIP",
    period: str = None,
    record: bool = False,
    current_user: dict = Depends(get_current_user),
):
    """Route wrapper: auth-gates then delegates to _compute_unified_settlement."""
    require_trip_access(trip_id, current_user)
    return _compute_unified_settlement(trip_id, mode=mode, period=period, record=record)


@app.get("/trip_settlements/{trip_id}")
def list_trip_settlements(trip_id: int, current_user: dict = Depends(get_current_user)):
    """
    List all recorded settlements for a given Trip.
    """
    require_trip_access(trip_id, current_user)
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
def get_trip_settlement_detail(settlement_id: int, current_user: dict = Depends(get_current_user)):
    """
    Retrieve details for a specific recorded trip settlement.
    Includes each family's contribution and balance.
    """
    require_trip_access(trip_id_for_trip_settlement(settlement_id), current_user)
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







@app.get("/download_pdf/{trip_id}")
def download_pdf(trip_id: int, current_user: dict = Depends(get_current_user)):
    """Admin-only minimal PDF download."""
    require_trip_access(trip_id, current_user)
    file_path = generate_settlement_pdf(trip_id)
    return FileResponse(path=file_path, filename=os.path.basename(file_path), media_type="application/pdf")




@app.get("/share_pdf/{trip_id}")
def share_pdf(trip_id: int, current_user: dict = Depends(get_current_user)):
    """Generate and share a WhatsApp link for this report."""
    require_trip_access(trip_id, current_user)
    return share_pdf_via_whatsapp(trip_id)


@app.get("/settlement_snapshot/{trip_id}")
def settlement_snapshot(trip_id: int, current_user: dict = Depends(get_current_user)):
    """
    Returns REAL settlement data exactly as seen on the Settlement page.
    No more stale DB snapshots.
    """
    require_trip_access(trip_id, current_user)

    print(f"📗 Generating LIVE snapshot for report (trip={trip_id}, mode=STAY)")

    # Call the shared plain helper directly (not the route function) — see
    # _compute_unified_settlement for why.
    data = _compute_unified_settlement(
        trip_id=trip_id,
        mode="STAY",
        period=None,
        record=False
    )

    # Remove unnecessary keys before sending to Flutter
    data.pop("message", None)
    data.pop("recorded_settlement_id", None)

    return data

@app.post("/group/create")
async def route_group_create(request: Request, current_user: dict = Depends(get_current_user)):
    return await group_create(request, current_user)

@app.get("/group/details")
async def route_group_details(group_id: int, current_user: dict = Depends(get_current_user)):
    return await group_get_details(group_id, current_user)

# @app.get("/group/current")
# async def route_group_current(user_id: int | None = None):
#     return await group_get_current(user_id)

@app.post("/group/add_expense")
async def route_group_add_expense(request: Request, current_user: dict = Depends(get_current_user)):
    return await group_add_expense(request, current_user)

@app.post("/group/delete_expense")
async def route_group_delete_expense(request: Request, current_user: dict = Depends(get_current_user)):
    return await group_delete_expense(request, current_user)

@app.post("/group/update_participants")
async def route_group_update_participants(request: Request, current_user: dict = Depends(get_current_user)):
    return await group_update_participants(request, current_user)

@app.post("/group/join")
async def route_group_join(request: Request, current_user: dict = Depends(get_current_user)):
    return await group_join(request, current_user)

@app.post("/group/edit_expense")
async def route_group_edit_expense(request: Request, current_user: dict = Depends(get_current_user)):

    return await group_edit_expense(request, current_user)

@app.post("/group/update_initial_fund")
async def route_group_update_initial_fund(request: Request, current_user: dict = Depends(get_current_user)):

    return await group_update_initial_fund(request, current_user)
@app.post("/group/exit")
async def route_group_exit(request: Request, current_user: dict = Depends(get_current_user)):
    return await group_exit(request, current_user)

@app.post("/group/delete")
async def route_group_delete(request: Request, current_user: dict = Depends(get_current_user)):

    return await group_delete(request, current_user)
@app.get("/group/list")
async def route_group_list(current_user: dict = Depends(get_current_user)):
    return await group_get_all(current_user)