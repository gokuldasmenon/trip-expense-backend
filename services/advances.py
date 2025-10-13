from database import get_connection
import psycopg2.extras


# ✅ Add Advance Record
def add_advance(trip_id, payer_id, receiver_id, amount, date):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO advances (trip_id, payer_family_id, receiver_family_id, amount, date, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        RETURNING id
    """, (trip_id, payer_id, receiver_id, amount, date))
    new_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Advance recorded successfully", "advance_id": new_id}



# ✅ Get All Advances for a Trip
def get_advances(trip_id):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("""
        SELECT 
            a.id,
            a.amount,
            a.date,
            f1.family_name AS payer_name,
            f2.family_name AS receiver_name
        FROM advances a
        LEFT JOIN family_details f1 ON a.payer_family_id = f1.id
        LEFT JOIN family_details f2 ON a.receiver_family_id = f2.id
        WHERE a.trip_id = %s
        ORDER BY a.date DESC
    """, (trip_id,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {"advances": rows}
def update_advance(advance_id, payer_id, receiver_id, amount, date):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE advances
        SET 
            payer_family_id = %s,
            receiver_family_id = %s,
            amount = %s,
            date = %s,
            updated_at = NOW()
        WHERE id = %s
    """, (payer_id, receiver_id, amount, date, advance_id))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Advance updated successfully", "advance_id": advance_id}
