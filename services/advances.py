from database import get_connection

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


def update_advance(advance_id, payer_id, receiver_id, amount, date):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE advances
        SET payer_family_id = %s,
            receiver_family_id = %s,
            amount = %s,
            date = %s,
            updated_at = NOW()
        WHERE id = %s
    """, (payer_id, receiver_id, amount, date, advance_id))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Advance updated successfully"}


def delete_advance(advance_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM advances WHERE id = %s", (advance_id,))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Advance deleted successfully"}
