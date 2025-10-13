from database import get_connection
from models import AdvanceModel

def add_advance(trip_id, payer_id, receiver_id, amount, date):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO advances (trip_id, payer_family_id, receiver_family_id, amount, date)
        VALUES (?, ?, ?, ?, ?)
    """, (trip_id, payer_id, receiver_id, amount, date))
    conn.commit()
    conn.close()
    return {"message": "Advance recorded successfully"}

def get_advances(trip_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT a.id, a.amount, a.date,
               f1.family_name AS payer_name,
               f2.family_name AS receiver_name
        FROM advances a
        LEFT JOIN family_details f1 ON a.payer_family_id = f1.id
        LEFT JOIN family_details f2 ON a.receiver_family_id = f2.id
        WHERE a.trip_id = ?
        ORDER BY a.date DESC
    """, (trip_id,))
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {"advances": rows}
