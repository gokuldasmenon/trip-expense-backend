from database import get_connection
import psycopg2.extras
import random, string
def generate_access_code(length=6):
    """Generate a random uppercase alphanumeric access code."""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def add_trip(name, start_date, trip_type):
    conn = get_connection()
    cursor = conn.cursor()

    access_code = generate_access_code()

    cursor.execute("""
        INSERT INTO trips (name, start_date, trip_type, access_code, status)
        VALUES (%s, %s, %s, %s, 'ACTIVE')
        RETURNING id, name, access_code
    """, (name, start_date, trip_type, access_code))

    trip = cursor.fetchone()
    conn.commit()
    conn.close()

    return {
        "message": "Trip created successfully",
        "trip_id": trip[0],
        "trip_name": trip[1],
        "access_code": trip[2]
    }


def get_trip_by_code(access_code):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM trips WHERE access_code = %s", (access_code,))
    trip = cursor.fetchone()
    conn.close()
    return trip

def join_trip_by_code(access_code):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT id, name, start_date, trip_type, access_code, status
        FROM trips
        WHERE access_code = %s AND status = 'ACTIVE'
    """, (access_code,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        raise Exception("Invalid or inactive trip code")
    return {"trip": dict(row)}

def get_all_trips():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM trips WHERE status='ACTIVE' ORDER BY id DESC")
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {"trips": rows}

def get_archived_trips():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM trips WHERE status='Archived' ORDER BY id DESC")
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {"trips": rows}

def archive_trip(trip_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE trips SET status='Archived' WHERE id=%s", (trip_id,))
    conn.commit()
    conn.close()
    return {"message": "Trip archived successfully"}

def restore_trip(trip_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE trips SET status='ACTIVE' WHERE id=%s", (trip_id,))
    conn.commit()
    conn.close()
    return {"message": "Trip restored successfully"}

def delete_trip(trip_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM expenses WHERE trip_id=%s", (trip_id,))
    cursor.execute("DELETE FROM family_details WHERE trip_id=%s", (trip_id,))
    cursor.execute("DELETE FROM trips WHERE id=%s", (trip_id,))
    conn.commit()
    conn.close()
    return {"message": "Trip deleted successfully"}
