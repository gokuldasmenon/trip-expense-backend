import os
import psycopg2
import psycopg2.extras

DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set. Please configure environment variable.")
    conn = psycopg2.connect(DATABASE_URL, sslmode="require")
    return conn

def initialize_database():
    conn = get_connection()
    cur = conn.cursor()
    # ✅ Create all tables
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trips (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        start_date TEXT,
        trip_type TEXT,
        access_code TEXT UNIQUE,
        status TEXT DEFAULT 'ACTIVE'
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS family_details (
        id SERIAL PRIMARY KEY,
        trip_id INTEGER REFERENCES trips(id) ON DELETE CASCADE,
        family_name TEXT NOT NULL,
        members_count INTEGER NOT NULL DEFAULT 0
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS expenses (
        id SERIAL PRIMARY KEY,
        trip_id INTEGER REFERENCES trips(id) ON DELETE CASCADE,
        payer_family_id INTEGER REFERENCES family_details(id) ON DELETE SET NULL,
        expense_name TEXT NOT NULL,
        amount NUMERIC(12,2) NOT NULL DEFAULT 0,
        date TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS advances (
        id SERIAL PRIMARY KEY,
        trip_id INTEGER REFERENCES trips(id) ON DELETE CASCADE,
        payer_family_id INTEGER REFERENCES family_details(id) ON DELETE CASCADE,
        receiver_family_id INTEGER REFERENCES family_details(id) ON DELETE CASCADE,
        amount NUMERIC(12,2) NOT NULL,
        date TEXT
    );
    """)
    conn.commit()
    cur.close()
    conn.close()
