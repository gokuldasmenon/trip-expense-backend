"""
auth.py — Authentication & authorization for the Expense Tracker API.

AUTH MODEL
----------
Phone number + OTP (one-time SMS code) -> short-lived JWT access token.
No passwords are stored anywhere. Every request that touches a specific
trip or group must carry a valid JWT in the "Authorization: Bearer <token>"
header, and the caller must actually be a member (or owner/creator) of the
trip/group they're asking about — the server checks this on every request,
it no longer trusts a client-supplied user_id.

SMS DELIVERY
------------
If TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN / TWILIO_FROM_NUMBER are all set
as environment variables, OTPs are sent via Twilio SMS.

If they are NOT set (e.g. local development, or before you've set up a paid
SMS account), the OTP is printed to the server log AND returned in the
POST /auth/request_otp response as "debug_otp" — clearly marked as a dev
convenience — so the whole login flow can be built and tested without
paying for SMS. The moment real Twilio credentials are configured,
debug_otp stops being returned and real texts go out instead.

REQUIRED ENVIRONMENT VARIABLE
------------------------------
JWT_SECRET_KEY  — a long random string. MUST be set in production (Render).
                  If it's missing, the app still runs (so local dev isn't
                  blocked) but logs a loud warning and uses an insecure
                  placeholder — every token issued under that placeholder
                  is forgeable, so this is not safe to deploy with.

OPTIONAL ENVIRONMENT VARIABLES
-------------------------------
ACCESS_TOKEN_TTL_DAYS   — how long a login stays valid (default 30)
TWILIO_ACCOUNT_SID
TWILIO_AUTH_TOKEN
TWILIO_FROM_NUMBER
"""

import os
import time
import random
import hmac
import hashlib
from datetime import datetime, timedelta

import jwt  # PyJWT
from fastapi import Header, HTTPException, WebSocket
import psycopg2.extras

from database import get_connection

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
JWT_SECRET = os.environ.get("JWT_SECRET_KEY")
if not JWT_SECRET:
    JWT_SECRET = "dev-only-insecure-secret-CHANGE-ME"
    print(
        "⚠️  JWT_SECRET_KEY is not set in the environment — using an INSECURE "
        "development placeholder. Every login token issued right now is "
        "forgeable. Set a real JWT_SECRET_KEY before deploying to production."
    )

JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_TTL_DAYS = int(os.environ.get("ACCESS_TOKEN_TTL_DAYS", "30"))

OTP_TTL_MINUTES = 10
OTP_MAX_ATTEMPTS = 5

TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID")
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN")
TWILIO_FROM = os.environ.get("TWILIO_FROM_NUMBER")
SMS_PROVIDER_CONFIGURED = bool(TWILIO_SID and TWILIO_TOKEN and TWILIO_FROM)


# ------------------------------------------------------------------
# OTP: generate / send / verify
# ------------------------------------------------------------------
def _otp_hash(phone: str, code: str) -> str:
    # HMAC keyed on the JWT secret so a stolen DB row alone can't be used
    # to derive valid codes for other numbers, and the code isn't stored
    # in the clear.
    return hmac.new(JWT_SECRET.encode(), f"{phone}:{code}".encode(), hashlib.sha256).hexdigest()


def generate_and_store_otp(phone: str) -> str:
    """Creates a fresh 6-digit OTP for `phone`, replacing any previous one,
    and returns the plaintext code (caller is responsible for sending it)."""
    code = f"{random.randint(0, 999999):06d}"
    code_hash = _otp_hash(phone, code)
    expires_at = datetime.utcnow() + timedelta(minutes=OTP_TTL_MINUTES)

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM otp_codes WHERE phone = %s", (phone,))
        cur.execute(
            """
            INSERT INTO otp_codes (phone, code_hash, expires_at, attempts, created_at)
            VALUES (%s, %s, %s, 0, NOW())
            """,
            (phone, code_hash, expires_at),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return code


def send_otp_sms(phone: str, code: str) -> bool:
    """Sends the OTP by SMS if a provider is configured. Returns True if a
    real text was sent, False if it only went to the server log (dev mode)."""
    if SMS_PROVIDER_CONFIGURED:
        try:
            from twilio.rest import Client  # optional dependency, imported lazily

            client = Client(TWILIO_SID, TWILIO_TOKEN)
            client.messages.create(
                body=f"Your Trip Expense Tracker code is {code}. It expires in {OTP_TTL_MINUTES} minutes.",
                from_=TWILIO_FROM,
                to=phone,
            )
            return True
        except Exception as e:
            print(f"❌ Failed to send OTP via Twilio, falling back to log: {e}")

    print(
        f"📩 [DEV OTP] phone={phone} code={code}  "
        f"(no SMS provider configured — set TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN / "
        f"TWILIO_FROM_NUMBER as env vars to send real texts)"
    )
    return False


def verify_otp(phone: str, code: str) -> None:
    """Raises HTTPException on any failure. Consumes the OTP (deletes it) on
    success so it can never be replayed."""
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM otp_codes WHERE phone = %s", (phone,))
        row = cur.fetchone()

        if not row:
            raise HTTPException(
                status_code=400,
                detail="No OTP was requested for this number, or it already expired. Request a new one.",
            )

        if row["attempts"] >= OTP_MAX_ATTEMPTS:
            cur.execute("DELETE FROM otp_codes WHERE phone = %s", (phone,))
            conn.commit()
            raise HTTPException(status_code=429, detail="Too many incorrect attempts. Request a new OTP.")

        if datetime.utcnow() > row["expires_at"]:
            cur.execute("DELETE FROM otp_codes WHERE phone = %s", (phone,))
            conn.commit()
            raise HTTPException(status_code=400, detail="OTP expired. Request a new one.")

        if not hmac.compare_digest(_otp_hash(phone, code), row["code_hash"]):
            cur.execute("UPDATE otp_codes SET attempts = attempts + 1 WHERE phone = %s", (phone,))
            conn.commit()
            raise HTTPException(status_code=400, detail="Incorrect OTP.")

        # ✅ Correct — consume it so it can't be reused.
        cur.execute("DELETE FROM otp_codes WHERE phone = %s", (phone,))
        conn.commit()
    finally:
        cur.close()
        conn.close()


# ------------------------------------------------------------------
# JWT access tokens
# ------------------------------------------------------------------
def create_access_token(user_id: int, phone: str) -> str:
    now_ts = int(time.time())
    payload = {
        "sub": str(user_id),
        "phone": phone,
        "iat": now_ts,
        "exp": now_ts + ACCESS_TOKEN_TTL_DAYS * 86400,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Session expired. Please log in again.")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid authentication token.")


def _extract_bearer(authorization: str | None) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=401,
            detail="Missing or invalid Authorization header. Expected 'Bearer <token>'.",
        )
    return authorization.split(" ", 1)[1].strip()


def _load_user(user_id: int) -> dict | None:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT id, name, phone, email, created_at FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()
    return dict(row) if row else None


def get_current_user(authorization: str | None = Header(None)) -> dict:
    """FastAPI dependency — use as: current_user: dict = Depends(get_current_user)"""
    token = _extract_bearer(authorization)
    payload = decode_access_token(token)
    user = _load_user(int(payload["sub"]))
    if not user:
        raise HTTPException(status_code=401, detail="User no longer exists.")
    return user


async def get_current_user_ws(websocket: WebSocket) -> dict | None:
    """Same idea, for the WebSocket handshake — there's no header, so the
    token is passed as ?token=... . Never raises; returns None on any
    failure so the caller can close the socket cleanly."""
    token = websocket.query_params.get("token")
    if not token:
        return None
    try:
        payload = decode_access_token(token)
    except HTTPException:
        return None
    return _load_user(int(payload["sub"]))


# ------------------------------------------------------------------
# Trip ownership / membership
# ------------------------------------------------------------------
def is_trip_member(trip_id: int, user_id: int) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM trips WHERE id = %s AND owner_id = %s", (trip_id, user_id))
        if cur.fetchone():
            return True
        cur.execute("SELECT 1 FROM trip_members WHERE trip_id = %s AND user_id = %s", (trip_id, user_id))
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def is_trip_owner(trip_id: int, user_id: int) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM trips WHERE id = %s AND owner_id = %s", (trip_id, user_id))
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def require_trip_access(trip_id: int, user: dict) -> None:
    """Any member (owner or joined participant) may proceed."""
    if not is_trip_member(trip_id, user["id"]):
        raise HTTPException(status_code=403, detail="You don't have access to this trip.")


def require_trip_owner(trip_id: int, user: dict) -> None:
    """Only the trip owner may proceed (archiving/deleting/restoring the trip itself)."""
    if not is_trip_owner(trip_id, user["id"]):
        raise HTTPException(status_code=403, detail="Only the trip owner can do this.")


def _lookup_fk(table: str, id_column: str, resource_id: int, fk_column: str) -> int:
    """Looks up `fk_column` for the row `id_column = resource_id` in `table`.
    Table/column names here are fixed, developer-controlled strings (never
    user input), so this is safe despite the f-string."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT {fk_column} FROM {table} WHERE {id_column} = %s", (resource_id,))
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Resource not found.")
    return row[0]


def trip_id_for_family(family_id: int) -> int:
    return _lookup_fk("family_details", "id", family_id, "trip_id")


def trip_id_for_expense(expense_id: int) -> int:
    return _lookup_fk("expenses", "id", expense_id, "trip_id")


def trip_id_for_advance(advance_id: int) -> int:
    return _lookup_fk("advances", "id", advance_id, "trip_id")


def trip_id_for_settlement_txn(txn_id: int) -> int:
    return _lookup_fk("settlement_transactions", "id", txn_id, "trip_id")


def trip_id_for_stay_settlement(settlement_id: int) -> int:
    return _lookup_fk("stay_settlements", "id", settlement_id, "trip_id")


def trip_id_for_trip_settlement(settlement_id: int) -> int:
    return _lookup_fk("trip_settlements", "id", settlement_id, "trip_id")


def trip_id_for_carry_forward_log(log_id: int) -> int:
    return _lookup_fk("stay_carry_forward_log", "id", log_id, "trip_id")


# ------------------------------------------------------------------
# Group Trip ownership / membership
# ------------------------------------------------------------------
def is_group_member(group_id: int, user_id: int) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM group_trip WHERE id = %s AND created_by = %s", (group_id, user_id))
        if cur.fetchone():
            return True
        cur.execute("SELECT 1 FROM group_participants WHERE group_id = %s AND user_id = %s", (group_id, user_id))
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def is_group_creator(group_id: int, user_id: int) -> bool:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM group_trip WHERE id = %s AND created_by = %s", (group_id, user_id))
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def require_group_access(group_id: int, user: dict) -> None:
    if not is_group_member(group_id, user["id"]):
        raise HTTPException(status_code=403, detail="You don't have access to this group trip.")


def require_group_creator(group_id: int, user: dict) -> None:
    if not is_group_creator(group_id, user["id"]):
        raise HTTPException(status_code=403, detail="Only the group creator can do this.")


def group_id_for_group_expense(expense_id: int) -> int:
    return _lookup_fk("group_expense", "id", expense_id, "group_id")
