import os
import psycopg2
import psycopg2.extras
from urllib.parse import urlparse


# ============================================================
# ✅ 1. Get DATABASE_URL (from Render, Railway, Supabase, etc.)
# ============================================================
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    # 🔹 Fallback for local dev (optional)
    # Example local Postgres URL: postgresql://user:password@localhost:5432/expensetracker
    DATABASE_URL = "postgresql://postgres:yourpassword@localhost:5432/expensetracker"
    print("⚠️ DATABASE_URL not set. Using local fallback.")


# ============================================================
# ✅ 2. Connection Utility
# ============================================================
def get_connection():
    """Create a PostgreSQL database connection with SSL if hosted."""
    try:
        # Enable SSL for cloud platforms like Render
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        raise RuntimeError("Unable to connect to the database")


# ============================================================
# ✅ 3. Initialize All Tables (idempotent)
# ============================================================
def initialize_database():
    conn = get_connection()
    cur = conn.cursor()

    # ✅ Trips Table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trips (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        start_date TEXT,
        trip_type TEXT,
        mode TEXT DEFAULT 'TRIP',              -- 🆕 Trip/Stay mode
        billing_cycle TEXT,                    -- 🆕 For STAY (optional)
        access_code TEXT UNIQUE,
        status TEXT DEFAULT 'ACTIVE',
        owner_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
        owner_name TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # ✅ Family Details Table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS family_details (
        id SERIAL PRIMARY KEY,
        trip_id INTEGER REFERENCES trips(id) ON DELETE CASCADE,
        family_name TEXT NOT NULL,
        members_count INTEGER NOT NULL DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # ✅ Expenses Table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS expenses (
        id SERIAL PRIMARY KEY,
        trip_id INTEGER REFERENCES trips(id) ON DELETE CASCADE,
        payer_family_id INTEGER REFERENCES family_details(id) ON DELETE SET NULL,
        expense_name TEXT NOT NULL,
        amount NUMERIC(12,2) NOT NULL DEFAULT 0,
        date TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # ✅ Advances Table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS advances (
        id SERIAL PRIMARY KEY,
        trip_id INTEGER REFERENCES trips(id) ON DELETE CASCADE,
        payer_family_id INTEGER REFERENCES family_details(id) ON DELETE CASCADE,
        receiver_family_id INTEGER REFERENCES family_details(id) ON DELETE CASCADE,
        amount NUMERIC(12,2) NOT NULL,
        date TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    conn.commit()
    cur.close()
    conn.close()

        # ✅ Stay Settlements Table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stay_settlements (
        id SERIAL PRIMARY KEY,
        trip_id INTEGER REFERENCES trips(id) ON DELETE CASCADE,
        mode TEXT DEFAULT 'STAY',
        period_start DATE,
        period_end DATE,
        total_expense NUMERIC(12,2),
        per_head_cost NUMERIC(12,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # ✅ Stay Settlement Details Table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stay_settlement_details (
        id SERIAL PRIMARY KEY,
        settlement_id INTEGER REFERENCES stay_settlements(id) ON DELETE CASCADE,
        family_id INTEGER REFERENCES family_details(id) ON DELETE CASCADE,
        family_name TEXT,
        members_count INTEGER,
        total_spent NUMERIC(12,2),
        due_amount NUMERIC(12,2),
        balance NUMERIC(12,2)
    );
    """)


