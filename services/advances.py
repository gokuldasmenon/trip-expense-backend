from database import get_connection
import psycopg2.extras

def add_advance(trip_id, payer_id, receiver_id, amount, date):
    """Record an advance payment between two families."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO advances (trip_id, payer_family_id, receiver_family_id, amount, date)
            VALUES (%s, %s, %s, %s, %s)
        """, (trip_id, payer_id, receiver_id, amount, date))
        conn.commit()
        return {"message": "Advance recorded successfully"}
    except Exception as e:
        conn.rollback()
        print(f"Error adding advance: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()


def add_advance(trip_id, payer_id, receiver_id, amount, date):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO advances (trip_id, payer_family_id, receiver_family_id, amount, date)
        VALUES (%s, %s, %s, %s, %s)
    """, (trip_id, payer_id, receiver_id, amount, date))
    conn.commit()
    conn.close()
    return {"message": "Advance recorded successfully"}

