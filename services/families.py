from database import get_connection
import psycopg2.extras


def add_family(trip_id, family_name, members_count):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO family_details (trip_id, family_name, members_count, updated_at)
        VALUES (%s, %s, %s, NOW())
        RETURNING id
    """, (trip_id, family_name, members_count))
    new_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Family added successfully", "family_id": new_id}



def get_families(trip_id):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("""
        SELECT id, family_name, members_count
        FROM family_details
        WHERE trip_id = %s
        ORDER BY id ASC
    """, (trip_id,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {"families": rows}


def update_family(family_id, family_name, members_count):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE family_details
        SET 
            family_name = %s, 
            members_count = %s,
            updated_at = NOW()
        WHERE id = %s
    """, (family_name, members_count, family_id))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Family updated successfully", "family_id": family_id}



def delete_family(family_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM family_details WHERE id = %s", (family_id,))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Family deleted successfully"}
