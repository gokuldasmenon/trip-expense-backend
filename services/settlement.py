
from database import get_connection
import psycopg2.extras
from datetime import datetime

def get_settlement(trip_id: int, start_date: str = None, end_date: str = None, record: bool = False):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Optional date range filter for stay settlements
    date_filter = ""
    if start_date and end_date:
        date_filter = "AND e.date BETWEEN %s AND %s"
        date_params = (trip_id, start_date, end_date)
    else:
        date_params = (trip_id,)

    # --- Step 1: Get all families ---
    cursor.execute("""
        SELECT id, family_name, members_count
        FROM family_details
        WHERE trip_id = %s
    """, (trip_id,))
    families = cursor.fetchall()

    if not families:
        cursor.close()
        conn.close()
        return {"message": "No families found for this trip."}

    family_ids = [f["id"] for f in families]
    family_names = {f["id"]: f["family_name"] for f in families}
    family_members = {f["id"]: f["members_count"] for f in families}

    # --- Step 2: Get expenses (with optional date filter) ---
    query = f"""
        SELECT payer_family_id, amount
        FROM expenses e
        WHERE trip_id = %s {date_filter}
    """
    cursor.execute(query, date_params)
    expenses = cursor.fetchall()

    expense_balance = {fid: 0.0 for fid in family_ids}
    total_expense = 0.0

    for e in expenses:
        payer_id = e["payer_family_id"]
        amt = float(e["amount"])
        if payer_id in expense_balance:
            expense_balance[payer_id] += amt
            total_expense += amt

    # --- Step 3: Compute per-head cost ---
    total_members = sum(family_members.values())
    per_head_cost = total_expense / total_members if total_members > 0 else 0.0
    expected_share = {fid: family_members[fid] * per_head_cost for fid in family_ids}

    # --- Step 4: Get advances ---
    cursor.execute("""
        SELECT payer_family_id, receiver_family_id, amount
        FROM advances
        WHERE trip_id = %s
    """, (trip_id,))
    advances = cursor.fetchall()

    advance_balance = {fid: 0.0 for fid in family_ids}
    for a in advances:
        payer_id = a["payer_family_id"]
        receiver_id = a["receiver_family_id"]
        amt = float(a["amount"])
        if payer_id in advance_balance:
            advance_balance[payer_id] += amt
        if receiver_id in advance_balance:
            advance_balance[receiver_id] -= amt

    # --- Step 5: Compute balances ---
    family_results = []
    for fid in family_ids:
        paid = expense_balance.get(fid, 0.0)
        owed = expected_share.get(fid, 0.0)
        adv = advance_balance.get(fid, 0.0)
        net = paid - owed + adv
        family_results.append({
            "family_id": fid,
            "family_name": family_names[fid],
            "members_count": family_members[fid],
            "total_spent": round(paid, 2),
            "raw_balance": round(net, 2),
            "balance": round(net, 2)
        })

    # --- Step 6: Transactions ---
    debtors = [f for f in family_results if f["balance"] < 0]
    creditors = [f for f in family_results if f["balance"] > 0]
    transactions = []

    for d in debtors:
        owed = abs(d["balance"])
        for c in creditors:
            if owed <= 0:
                break
            if c["balance"] <= 0:
                continue
            payment = min(owed, c["balance"])
            transactions.append({
                "from": d["family_name"],
                "to": c["family_name"],
                "amount": round(payment, 2)
            })
            owed -= payment
            c["balance"] -= payment

    for f in family_results:
        f["balance"] = f["raw_balance"]

    cursor.close()
    conn.close()

    return {
        "total_expense": round(total_expense, 2),
        "total_members": total_members,
        "per_head_cost": round(per_head_cost, 2),
        "families": family_results,
        "transactions": transactions if transactions else "All accounts settled"
    }



def get_trip_summary(trip_id: int):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # --- Trip info ---
    cursor.execute("SELECT * FROM trips WHERE id = %s", (trip_id,))
    trip = cursor.fetchone()
    if not trip:
        cursor.close()
        conn.close()
        return {"error": f"Trip with id {trip_id} not found"}

    # --- Families ---
    cursor.execute("""
        SELECT id, family_name, members_count
        FROM family_details
        WHERE trip_id = %s
    """, (trip_id,))
    families = cursor.fetchall()

    # --- Expenses ---
    cursor.execute("""
        SELECT e.expense_name, e.amount, e.date, f.family_name AS payer
        FROM expenses e
        JOIN family_details f ON e.payer_family_id = f.id
        WHERE e.trip_id = %s
        ORDER BY e.date ASC, e.id ASC
    """, (trip_id,))
    expenses = cursor.fetchall()

    cursor.close()
    conn.close()

    settlement_data = get_settlement(trip_id)

    return {
        "trip": trip,
        "families": families,
        "expenses": expenses,
        "settlement": settlement_data
    }
from datetime import date, timedelta, datetime
from database import get_connection
import psycopg2.extras


# ===============================================
# 🕒 PERIOD DETERMINATION
# ===============================================
def determine_period(period_type: str):
    """Returns (start_date, end_date) for given period type."""
    today = date.today()

    if period_type == "weekly":
        start = today - timedelta(days=today.weekday())  # Monday
        end = start + timedelta(days=6)
    elif period_type == "monthly":
        start = today.replace(day=1)
        next_month = start.replace(day=28) + timedelta(days=4)
        end = next_month - timedelta(days=next_month.day)
    else:
        # on_demand: same-day period
        start = end = today

    return (start, end)


# ===============================================
# 🔁 FETCH LAST STAY SETTLEMENT
# ===============================================
def get_last_stay_settlement(trip_id: int):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("""
        SELECT id, period_start, period_end, created_at
        FROM stay_settlements
        WHERE trip_id = %s
        ORDER BY id DESC
        LIMIT 1
    """, (trip_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result


# ===============================================
# 💰 CALCULATE STAY SETTLEMENT
# ===============================================
from datetime import datetime
import psycopg2.extras

from datetime import datetime
import psycopg2.extras

def calculate_stay_settlement(trip_id: int):
    """
    Calculates stay settlement for a trip with carry-forward and payment adjustments.
    Ensures Adjusted balances reflect applied settlement transactions.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # 1️⃣ Get total expense and per-head cost
    cursor.execute("SELECT COALESCE(SUM(amount), 0) AS total_expense FROM expenses WHERE trip_id = %s;", (trip_id,))
    total_expense = float(cursor.fetchone()["total_expense"] or 0)

    cursor.execute("SELECT COALESCE(SUM(members_count), 0) AS total_members FROM family_details WHERE trip_id = %s;", (trip_id,))
    total_members = int(cursor.fetchone()["total_members"] or 1)
    per_head_cost = round(total_expense / total_members, 2)

    # 2️⃣ Get previous settlement
    cursor.execute("""
        SELECT id, period_end
        FROM stay_settlements
        WHERE trip_id = %s
        ORDER BY id DESC LIMIT 1;
    """, (trip_id,))
    prev = cursor.fetchone()
    prev_settlement_id = prev["id"] if prev else None
    prev_end_date = prev["period_end"] if prev else None

    # 3️⃣ Load previous balances (carry-forward)
    previous_balance_map = {}
    if prev_settlement_id:
        cursor.execute("""
            SELECT family_id, balance
            FROM stay_settlement_details
            WHERE settlement_id = %s;
        """, (prev_settlement_id,))
        for row in cursor.fetchall():
            previous_balance_map[row["family_id"]] = float(row["balance"])

    # 4️⃣ Compute each family’s net balance
    cursor.execute("SELECT id AS family_id, family_name, members_count FROM family_details WHERE trip_id = %s;", (trip_id,))
    families = cursor.fetchall()

    results = []
    for f in families:
        family_id = f["family_id"]
        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0) AS spent
            FROM expenses
            WHERE trip_id = %s AND payer_family_id = %s;
        """, (trip_id, family_id))
        spent = float(cursor.fetchone()["spent"] or 0)
        due = round(per_head_cost * f["members_count"], 2)
        balance = round(spent - due, 2)
        prev_balance = previous_balance_map.get(family_id, 0.0)

        results.append({
            "family_id": family_id,
            "family_name": f["family_name"],
            "members_count": f["members_count"],
            "total_spent": spent,
            "due_amount": due,
            "previous_balance": prev_balance,
            "balance": balance,
        })

    # 5️⃣ Fetch active settlement transactions (adjustments)
    cursor.execute("""
        SELECT from_family_id, to_family_id, amount
        FROM settlement_transactions
        WHERE trip_id = %s;
    """, (trip_id,))
    transactions = cursor.fetchall()

    # 6️⃣ Build adjustment map
    adjustments = {}
    for txn in transactions:
        f_from = txn["from_family_id"]
        f_to = txn["to_family_id"]
        amt = float(txn["amount"])
        adjustments[f_from] = adjustments.get(f_from, 0.0) - amt   # payer gets +amt
        adjustments[f_to] = adjustments.get(f_to, 0.0) - amt       # receiver gets -amt

    # 7️⃣ Apply adjustment per family
    for f in results:
        fid = f["family_id"]
        adj = adjustments.get(fid, 0.0)
        f["adjusted_balance"] = round(f["balance"] + adj, 2)

    # 8️⃣ Compute period
    period_start = prev_end_date if prev_end_date else datetime.utcnow().date()
    period_end = datetime.utcnow().date()

    conn.close()

    print(f"✅ Settlement computed for trip {trip_id}: adjustments={adjustments}")

    return {
        "period_start": period_start,
        "period_end": period_end,
        "total_expense": total_expense,
        "total_members": total_members,
        "per_head_cost": per_head_cost,
        "families": results,
        "carry_forward": bool(previous_balance_map),
        "carry_forward_breakdown": [
            {"family_id": fid, "previous_balance": bal}
            for fid, bal in previous_balance_map.items()
        ],
        "previous_settlement_id": prev_settlement_id,
        "transactions": transactions,
    }



def record_carry_forward_log(trip_id: int, previous_settlement_id: int, new_settlement_id: int, families: list):
    """
    Logs carry-forward balances for each family from the previous to the new stay settlement.
    Failures here are logged but do not block the main settlement.
    """
    if not previous_settlement_id:
        print("ℹ️ No previous settlement found — skipping carry-forward log.")
        return

    try:
        conn = get_connection()
        cursor = conn.cursor()

        print(f"🧾 Recording carry-forward log for trip {trip_id} (prev={previous_settlement_id}, new={new_settlement_id})")

        for fam in families:
            family_id = fam["family_id"]
            prev_balance = float(fam.get("previous_balance", 0.0))
            new_balance = float(fam.get("balance", 0.0))
            delta = round(new_balance - prev_balance, 2)

            cursor.execute("""
                INSERT INTO stay_carry_forward_log (
                    trip_id, previous_settlement_id, new_settlement_id,
                    family_id, previous_balance, new_balance, delta
                ) VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (trip_id, previous_settlement_id, new_settlement_id, family_id, prev_balance, new_balance, delta))

        conn.commit()
        conn.close()
        print("✅ Carry-forward log recorded successfully.")

    except Exception as e:
        import traceback
        print(f"⚠️ Warning: carry-forward log failed — continuing anyway. Error: {e}")
        traceback.print_exc()










# ===============================================
# 🧮 ORCHESTRATOR (USED BY /settlement/{trip_id})
# ===============================================
def get_stay_settlement(trip_id: int, period="on_demand", record=False):
    start_date, end_date = determine_period(period)
    result = calculate_stay_settlement(trip_id, start_date, end_date, carry_forward=True)

    if record:
        settlement_id = record_stay_settlement(trip_id, result)
        result["recorded_id"] = settlement_id
    result["carry_forward_total"] = round(sum(f["previous_balance"] for f in result["families"]), 2)
    return result

def record_stay_settlement(trip_id: int, result: dict):
    """
    Finalizes a stay settlement:
    - Saves summary and per-family balances (adjusted for payments)
    - Records carry-forward log (if any)
    - Archives active transactions
    - Returns new settlement_id
    """
    conn = get_connection()
    cursor = conn.cursor()

    print(f"🧾 Finalizing stay settlement for trip {trip_id}...")

    # 0️⃣ Prevent double-finalization in same session
    cursor.execute("""
        SELECT id FROM stay_settlements 
        WHERE trip_id = %s
        ORDER BY id DESC LIMIT 1;
    """, (trip_id,))
    existing = cursor.fetchone()

    if existing and result.get("previous_settlement_id") == existing[0]:
        print(f"⚠️ Settlement for trip {trip_id} already finalized recently — skipping duplicate.")
        conn.close()
        return existing[0]

    # 1️⃣ Insert into stay_settlements summary table
    cursor.execute("""
        INSERT INTO stay_settlements (
            trip_id, mode, period_start, period_end, total_expense, per_head_cost
        ) VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
    """, (
        trip_id,
        "STAY",
        result.get("period_start"),
        result.get("period_end"),
        result.get("total_expense"),
        result.get("per_head_cost")
    ))
    settlement_id = cursor.fetchone()[0]
    conn.commit()
    print(f"✅ Settlement summary saved (ID={settlement_id})")

    # 2️⃣ Insert family-level details — using adjusted balance after payments
    for fam in result["families"]:
        balance_value = fam.get("adjusted_balance", fam.get("balance", 0))
        cursor.execute("""
            INSERT INTO stay_settlement_details (
                settlement_id, family_id, family_name, members_count,
                total_spent, due_amount, balance
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (
            settlement_id,
            fam["family_id"],
            fam["family_name"],
            fam["members_count"],
            fam["total_spent"],
            fam["due_amount"],
            round(balance_value, 2)
        ))
    conn.commit()
    print("✅ Family-level settlement details saved.")

    # 3️⃣ Record carry-forward log after settlement details
    previous_settlement_id = result.get("previous_settlement_id")
    try:
        record_carry_forward_log(trip_id, previous_settlement_id, settlement_id, result["families"])
        print("📦 Carry-forward log recorded.")
    except Exception as e:
        print(f"⚠️ Carry-forward log skipped due to error: {e}")

    # 4️⃣ Archive settlement transactions after applying them
    try:
        cursor.execute("""
            INSERT INTO settlement_transactions_archive (
                trip_id, from_family_id, to_family_id, amount, transaction_date, remarks, settlement_id
            )
            SELECT trip_id, from_family_id, to_family_id, amount, transaction_date, remarks, %s
            FROM settlement_transactions
            WHERE trip_id = %s;
        """, (settlement_id, trip_id))

        archived_count = cursor.rowcount

        cursor.execute("DELETE FROM settlement_transactions WHERE trip_id = %s;", (trip_id,))
        conn.commit()
        print(f"🗃️ Archived and cleared {archived_count} settlement transaction(s) for trip_id={trip_id}")

    except Exception as e:
        print(f"⚠️ Failed to archive/clear settlement transactions — {e}")

    conn.close()
    print(f"🏁 Stay settlement completed successfully (ID={settlement_id})\n")
    return settlement_id






def record_trip_settlement(trip_id: int, result: dict) -> int:
    """
    Records a trip settlement into trip_settlements and trip_settlement_details.
    Returns the new settlement_id.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Insert into trip_settlements
    cursor.execute("""
        INSERT INTO trip_settlements (
            trip_id, mode, period_start, period_end, total_expense, per_head_cost
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (
        trip_id,
        result.get("mode", "TRIP"),
        result.get("period_start", datetime.utcnow().date()),
        result.get("period_end", datetime.utcnow().date()),
        result.get("total_expense", 0.0),
        result.get("per_head_cost", 0.0)
    ))
    settlement_id = cursor.fetchone()["id"]

    # Insert family-level details
    for fam in result.get("families", []):
        cursor.execute("""
            INSERT INTO trip_settlement_details (
                settlement_id, family_id, family_name, members_count, total_spent, due_amount, balance
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            settlement_id,
            fam.get("family_id"),
            fam.get("family_name"),
            fam.get("members_count"),
            fam.get("total_spent"),
            fam.get("raw_balance", 0.0),  # raw_balance acts as due_amount here
            fam.get("balance", 0.0)
        ))

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Trip settlement {settlement_id} recorded for trip {trip_id}")
    return settlement_id
