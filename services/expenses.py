from database import get_connection
import psycopg2.extras

def add_expense(trip_id, payer_id, name, amount, date):
    """Add a new expense record for a trip."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO expenses (trip_id, payer_family_id, expense_name, amount, date)
            VALUES (%s, %s, %s, %s, %s)
        """, (trip_id, payer_id, name, amount, date))
        conn.commit()
        return {"message": "Expense added successfully"}
    except Exception as e:
        conn.rollback()
        print(f"Error adding expense: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()


def get_expenses(trip_id):
    """Retrieve all expenses for a given trip."""
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("""
            SELECT 
                e.id,
                e.expense_name,
                e.amount,
                e.date,
                f.family_name AS payer
            FROM expenses e
            JOIN family_details f ON e.payer_family_id = f.id
            WHERE e.trip_id = %s
            ORDER BY e.date ASC, e.id ASC
        """, (trip_id,))
        rows = [dict(row) for row in cursor.fetchall()]
        return rows
    except Exception as e:
        print(f"Error fetching expenses: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()


def update_expense(expense_id, payer_id, name, amount, date):
    """Update an existing expense record."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE expenses
            SET payer_family_id = %s,
                expense_name = %s,
                amount = %s,
                date = %s
            WHERE id = %s
        """, (payer_id, name, amount, date, expense_id))
        conn.commit()
        if cursor.rowcount == 0:
            return {"error": f"No expense found with ID {expense_id}"}
        return {"message": "Expense updated successfully"}
    except Exception as e:
        conn.rollback()
        print(f"Error updating expense: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()


def delete_expense(expense_id):
    """Delete an expense record by ID."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM expenses WHERE id = %s", (expense_id,))
        conn.commit()
        if cursor.rowcount == 0:
            return {"error": f"No expense found with ID {expense_id}"}
        return {"message": "Expense deleted successfully"}
    except Exception as e:
        conn.rollback()
        print(f"Error deleting expense: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()
