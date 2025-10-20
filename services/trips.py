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


def get_trip_by_code(access_code):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM trips WHERE access_code = %s", (access_code,))
    trip = cursor.fetchone()
    conn.close()
    return trip

def get_archived_trips():
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT id, name, start_date, trip_type, access_code, status
        FROM trips
        WHERE status = 'Archived'
        ORDER BY id DESC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

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

def restore_trip(trip_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE trips SET status='ACTIVE' WHERE id=%s", (trip_id,))
    conn.commit()
    conn.close()
    return {"message": "Trip restored successfully"}

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
