from database import get_connection

def add_family(trip_id, family_name, members_count):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO family_details (trip_id, family_name, members_count)
        VALUES (%s, %s, %s)
    """, (trip_id, family_name, members_count))
    conn.commit()
    conn.close()
    return {"message": "Family added successfully"}

def get_families(trip_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, family_name, members_count
        FROM family_details
        WHERE trip_id = %s
    """, (trip_id,))
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {"families": rows}

def update_family(family_id, family_name, members_count):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE family_details
        SET family_name = %s, members_count = %s
        WHERE id = %s
    """, (family_name, members_count, family_id))
    conn.commit()
    conn.close()
    return {"message": "Family updated successfully"}

def delete_family(family_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM family_details WHERE id = %s", (family_id,))
    conn.commit()
    conn.close()
    return {"message": "Family deleted successfully"}
