from fastapi import HTTPException
from database import get_connection
import psycopg2.extras
import random, string
from database import get_connection
import random, string

def generate_access_code(length=6):
    """Generate a 6-character alphanumeric trip access code."""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))


def add_trip(name, start_date, trip_type, created_by="Owner"):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    access_code = generate_access_code()

    cursor.execute("""
        INSERT INTO trips (name, start_date, trip_type, access_code)
        VALUES (%s, %s, %s, %s)
        RETURNING id, name, start_date, trip_type, access_code
    """, (name, start_date, trip_type, access_code))

    trip = cursor.fetchone()

    # ✅ Record the trip creator as the owner
    cursor.execute("""
        INSERT INTO trip_participants (trip_id, user_name, role)
        VALUES (%s, %s, 'owner')
    """, (trip['id'], created_by))

    conn.commit()
    cursor.close()
    conn.close()

    return trip


def get_all_trips():
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("""
        SELECT id, name, start_date, trip_type, access_code
        FROM trips
        ORDER BY id DESC
    """)
    trips = cursor.fetchall()
    cursor.close()
    conn.close()
    return trips


def join_trip_by_code(access_code, user_name="Guest"):
    """Join an existing trip using its access code."""
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cursor.execute("SELECT * FROM trips WHERE access_code = %s", (access_code,))
    trip = cursor.fetchone()

    if not trip:
        cursor.close()
        conn.close()
        return None

    # ✅ Record the participant if not already joined
    cursor.execute("""
        INSERT INTO trip_participants (trip_id, user_name, role)
        VALUES (%s, %s, 'member')
        ON CONFLICT DO NOTHING
    """, (trip['id'], user_name))

    conn.commit()
    cursor.close()
    conn.close()

    return trip

def get_trips_for_user(user_id: int):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # 👑 Owned trips (exclude archived)
    cursor.execute("""
        SELECT id, name, start_date, trip_type, access_code, owner_name,
               created_at, mode, billing_cycle
        FROM trips
        WHERE owner_id = %s AND status='ACTIVE'
        ORDER BY id DESC
    """, (user_id,))
    own_trips = cursor.fetchall()

    # 🤝 Joined trips (exclude archived)
    cursor.execute("""
        SELECT t.id, t.name, t.start_date, t.trip_type, t.access_code, t.owner_name,
               t.created_at, t.mode, t.billing_cycle
        FROM trips t
        JOIN trip_members tm ON tm.trip_id = t.id
        WHERE tm.user_id = %s AND t.owner_id != %s AND t.status='ACTIVE'
        ORDER BY t.id DESC
    """, (user_id, user_id))
    joined_trips = cursor.fetchall()

    cursor.close()
    conn.close()
    return {"own_trips": own_trips, "joined_trips": joined_trips}


def get_archived_trips():
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("""
        SELECT * FROM trips
        WHERE status='ARCHIVED'
        ORDER BY id DESC
    """)
    trips = cursor.fetchall()
    cursor.close()
    conn.close()
    return {"trips": trips}

def archive_trip(trip_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE trips
            SET status = 'ARCHIVED', updated_at = NOW()
            WHERE id = %s
        """, (trip_id,))
        if cursor.rowcount == 0:
            conn.rollback()
            return {"message": f"Trip {trip_id} not found or already archived."}
        conn.commit()
        return {"message": f"Trip {trip_id} archived successfully."}
    except Exception as e:
        conn.rollback()
        raise Exception(f"Error archiving trip: {e}")
    finally:
        cursor.close()
        conn.close()

def restore_trip(trip_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE trips SET status='ACTIVE' WHERE id=%s", (trip_id,))
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Trip not found")
        conn.commit()
        return {"message": f"Trip {trip_id} restored successfully."}
    finally:
        cursor.close()
        conn.close()

# ✅ DELETE trip
def delete_trip(trip_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            DELETE FROM trips
            WHERE id = %s
        """, (trip_id,))
        if cursor.rowcount == 0:
            conn.rollback()
            return {"message": f"Trip {trip_id} not found."}
        conn.commit()
        return {"message": f"Trip {trip_id} deleted successfully."}
    except Exception as e:
        conn.rollback()
        raise Exception(f"Error deleting trip: {e}")
    finally:
        cursor.close()
        conn.close()
