from database import get_connection
import psycopg2.extras


def add_expense(trip_id, payer_id, name, amount, date):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO expenses (trip_id, payer_family_id, expense_name, amount, date, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        RETURNING id
    """, (trip_id, payer_id, name, amount, date))
    new_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Expense added successfully", "expense_id": new_id}
def get_expenses(trip_id):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("""
        SELECT 
            e.id,
            e.expense_name,
            e.amount,
            e.date,
            f.family_name AS payer
        FROM expenses e
        LEFT JOIN family_details f ON e.payer_family_id = f.id
        WHERE e.trip_id = %s
        ORDER BY e.date ASC, e.id ASC
    """, (trip_id,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def update_expense(expense_id, payer_id, name, amount, date):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE expenses
        SET payer_family_id = %s,
            expense_name = %s,
            amount = %s,
            date = %s,
            updated_at = NOW()
        WHERE id = %s
    """, (payer_id, name, amount, date, expense_id))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Expense updated successfully"}


def delete_expense(expense_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM expenses WHERE id = %s", (expense_id,))
    conn.commit()
    cursor.close()
    conn.close()
    return {"message": "Expense deleted successfully"}


