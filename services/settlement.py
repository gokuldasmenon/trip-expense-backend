
from database import get_connection
import psycopg2.extras
from datetime import datetime
import json
from datetime import date, timedelta, datetime ,timezone

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
            "total_spent": round(paid),
            "raw_balance": round(net),
            "balance": round(net)
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
                "amount": round(payment)
            })
            owed -= payment
            c["balance"] -= payment

    for f in family_results:
        f["balance"] = f["raw_balance"]

    cursor.close()
    conn.close()

    return {
        "total_expense": round(total_expense),
        "total_members": total_members,
        "per_head_cost": round(per_head_cost),
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
    STAY mode settlement:
    - Per-head cost based on total members
    - Net = previous_carry_forward + (spent - due)
    - Adjusted = Net + (sum of active transaction impacts)
    - Suggested settlements are derived from adjusted balances
    - Internal values use floats for accuracy; rounding only at output
    """

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # 1️⃣ Total expense, total members, per-head cost
    cursor.execute("SELECT COALESCE(SUM(amount), 0) AS total_expense FROM expenses WHERE trip_id = %s;", (trip_id,))
    total_expense = float(cursor.fetchone()["total_expense"] or 0.0)

    cursor.execute("SELECT COALESCE(SUM(members_count), 0) AS total_members FROM family_details WHERE trip_id = %s;", (trip_id,))
    total_members = int(cursor.fetchone()["total_members"] or 0) or 1
    per_head_cost = total_expense / total_members

    # 2️⃣ Previous settlement (for carry-forward)
    cursor.execute("""
        SELECT id, period_end
        FROM stay_settlements
        WHERE trip_id = %s
        ORDER BY id DESC LIMIT 1;
    """, (trip_id,))
    prev = cursor.fetchone()
    prev_settlement_id = prev["id"] if prev else None
    prev_end_date = prev["period_end"] if prev else None

    # 3️⃣ Carry-forward map (previous balances)
    previous_balance_map = {}
    if prev_settlement_id:
        cursor.execute("""
            SELECT family_id, balance
            FROM stay_settlement_details
            WHERE settlement_id = %s;
        """, (prev_settlement_id,))
        for row in cursor.fetchall():
            previous_balance_map[row["family_id"]] = float(row["balance"])

    # 4️⃣ Compute family balances
    cursor.execute("""
        SELECT id AS family_id, family_name, members_count
        FROM family_details
        WHERE trip_id = %s
        ORDER BY id;
    """, (trip_id,))
    families = cursor.fetchall()

    results = []
    for f in families:
        fid = f["family_id"]

        cursor.execute("""
            SELECT COALESCE(SUM(amount), 0) AS spent
            FROM expenses
            WHERE trip_id = %s AND payer_family_id = %s;
        """, (trip_id, fid))
        spent = float(cursor.fetchone()["spent"] or 0.0)
        due = per_head_cost * int(f["members_count"])
        prev_bal = previous_balance_map.get(fid, 0.0)

        net = prev_bal + (spent - due)

        results.append({
            "family_id": fid,
            "family_name": f["family_name"],
            "members_count": int(f["members_count"]),
            "total_spent": spent,
            "due_amount": due,
            "previous_balance": prev_bal,
            "balance": net,
        })
        print(f"🧮 [DEBUG] Family {f['family_name']}: spent={spent:.2f}, due={due:.2f}, prev={prev_bal:.2f}, net={net:.2f}")

    # 5️⃣ Load transactions (active and archived)
    cursor.execute("""
        SELECT t.from_family_id, f1.family_name AS from_family,
               t.to_family_id, f2.family_name AS to_family,
               t.amount, t.transaction_date, t.remarks
        FROM settlement_transactions t
        LEFT JOIN family_details f1 ON t.from_family_id = f1.id
        LEFT JOIN family_details f2 ON t.to_family_id = f2.id
        WHERE t.trip_id = %s
        ORDER BY t.id;
    """, (trip_id,))
    active_txns = cursor.fetchall()
    for txn in active_txns:
        txn["from"] = txn.get("from_family")
        txn["to"] = txn.get("to_family")

    cursor.execute("""
        SELECT sta.from_family_id, f1.family_name AS from_family,
               sta.to_family_id, f2.family_name AS to_family,
               sta.amount, sta.transaction_date, sta.remarks, sta.settlement_id
        FROM settlement_transactions_archive sta
        LEFT JOIN family_details f1 ON sta.from_family_id = f1.id
        LEFT JOIN family_details f2 ON sta.to_family_id = f2.id
        WHERE sta.trip_id = %s
          AND sta.settlement_id = (SELECT MAX(id) FROM stay_settlements WHERE trip_id = %s)
        ORDER BY sta.id;
    """, (trip_id, trip_id))
    archived_txns = cursor.fetchall()
    for txn in archived_txns:
        txn["from"] = txn.get("from_family")
        txn["to"] = txn.get("to_family")

    # 6️⃣ Apply adjustments (active transactions)
    txns_to_use = active_txns if active_txns else archived_txns
    adjustments = {f["family_id"]: 0.0 for f in results}

    for txn in txns_to_use:
        f_from, f_to = txn["from_family_id"], txn["to_family_id"]
        amt = float(txn["amount"])
        adjustments[f_from] = adjustments.get(f_from, 0.0) + amt
        adjustments[f_to] = adjustments.get(f_to, 0.0) - amt

    print("🔧 Adjustments applied (payer pays = balance increases):")
    for f in results:
        fid = f["family_id"]
        adj = adjustments.get(fid, 0.0)
        adjusted = f["balance"] + adj
        f["adjusted_balance"] = adjusted
        print(f"▶ {f['family_name']}: Net={f['balance']:.2f} + Adj({adj:+.2f}) = Adjusted={adjusted:.2f}")

    # ✅ Normalize total adjusted to exactly 0.00 (final precision correction)
    total_adj = sum(f["adjusted_balance"] for f in results)
    if abs(total_adj) > 0.01:
        correction = -total_adj
        target = max(results, key=lambda x: abs(x["adjusted_balance"]))
        target["adjusted_balance"] += correction
        print(f"🔧 Final correction {correction:+.2f} applied to {target['family_name']} (ensured total=0.00)")

    # 7️⃣ Suggested settlements
    creditors = [{"family_name": f["family_name"], "bal": f["adjusted_balance"]}
                 for f in results if f["adjusted_balance"] > 0.01]
    debtors = [{"family_name": f["family_name"], "bal": f["adjusted_balance"]}
               for f in results if f["adjusted_balance"] < -0.01]

    creditors.sort(key=lambda x: -x["bal"])
    debtors.sort(key=lambda x: x["bal"])

    suggested = []
    ci, di = 0, 0
    while ci < len(creditors) and di < len(debtors):
        pay_amt = min(creditors[ci]["bal"], -debtors[di]["bal"])
        if pay_amt <= 0:
            break
        suggested.append({
            "from": debtors[di]["family_name"],
            "to": creditors[ci]["family_name"],
            "amount": round(pay_amt)
        })
        creditors[ci]["bal"] -= pay_amt
        debtors[di]["bal"] += pay_amt
        if abs(creditors[ci]["bal"]) < 0.01:
            ci += 1
        if abs(debtors[di]["bal"]) < 0.01:
            di += 1

    # 8️⃣ Period and output formatting
    period_start = prev_end_date if prev_end_date else datetime.utcnow().date()
    period_end = datetime.utcnow().date()
    conn.close()

    # ✅ Round only for output (UI safety)
    for f in results:
        f["total_spent"] = round(f["total_spent"])
        f["due_amount"] = round(f["due_amount"])
        f["previous_balance"] = round(f["previous_balance"])
        f["balance"] = round(f["balance"])
        f["adjusted_balance"] = round(f["adjusted_balance"])

    return {
        "period_start": period_start,
        "period_end": period_end,
        "total_expense": round(total_expense),
        "total_members": total_members,
        "per_head_cost": round(per_head_cost),
        "families": results,
        "carry_forward": bool(previous_balance_map),
        "carry_forward_breakdown": [
            {"family_id": fid, "previous_balance": bal}
            for fid, bal in previous_balance_map.items()
        ],
        "previous_settlement_id": prev_settlement_id,
        "active_transactions": active_txns,
        "archived_transactions": archived_txns,
        "suggested": suggested,
    }







def record_stay_settlement(trip_id: int, result: dict):
    """
    Finalizes a stay settlement:
    - Saves summary and per-family balances (adjusted for payments)
    - Records carry-forward log (if any)
    - Archives active transactions
    - Returns new settlement_id
    """
    from datetime import datetime, timezone
    conn = get_connection()
    cursor = conn.cursor()

    print(f"🧾 Finalizing  n for trip {trip_id}...")

    # 🔍 0️⃣ Check for recent duplicate (avoid accidental double-click)
    cursor.execute("""
        SELECT id, created_at 
        FROM stay_settlements 
        WHERE trip_id = %s
        ORDER BY id DESC LIMIT 1;
    """, (trip_id,))
    existing = cursor.fetchone()

    prev_id = result.get("previous_settlement_id")
    last_id = existing[0] if existing else None
    print(f"🔍 Checking duplicate prevention: prev_id={prev_id}, last_settlement_in_db={last_id}")

    # ✅ Only skip if the *same settlement* was saved seconds ago
    if existing and existing[1]:
        created_time = existing[1].replace(tzinfo=None)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        seconds_since = (now - created_time).total_seconds()
        if seconds_since < 5:
            print(f"⚠️ Skipping immediate re-finalization for trip {trip_id} "
                f"(last settlement {seconds_since:.1f}s ago)")
            conn.close()
            return last_id   # <-- ✅ Now it runs only inside the if condition


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

    # 2️⃣ Insert family-level details
    for fam in result["families"]:
        balance_value = fam.get("balance", 0)
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
            round(balance_value)
        ))
    conn.commit()
    print("✅ Family-level settlement details saved.")

    # 3️⃣ Record carry-forward log after saving details
    previous_settlement_id = result.get("previous_settlement_id")
    try:
        print(f"🧾 Calling record_carry_forward_log(prev={previous_settlement_id}, new={settlement_id})")
        record_carry_forward_log(trip_id, previous_settlement_id, settlement_id, result["families"])

        # Verify insertion
        cursor.execute("SELECT COUNT(*) FROM stay_carry_forward_log WHERE trip_id = %s;", (trip_id,))
        log_count = cursor.fetchone()[0]
        print(f"🔍 [DEBUG] stay_carry_forward_log now has {log_count} rows for trip {trip_id}.")
    except Exception as e:
        print(f"⚠️ Carry-forward log skipped due to error: {e}")

    # 4️⃣ Archive and clear settlement transactions
    try:
        cursor.execute("""
            INSERT INTO settlement_transactions_archive (
                trip_id, from_family_id, to_family_id, amount, transaction_date, remarks, settlement_id
            )
            SELECT trip_id, from_family_id, to_family_id, amount, transaction_date, remarks, %s
            FROM settlement_transactions
            WHERE trip_id = %s;
        """, (settlement_id, trip_id))
        conn.commit()

        cursor.execute("DELETE FROM settlement_transactions WHERE trip_id = %s;", (trip_id,))
        conn.commit()

        print(f"📦 Archived and cleared settlement transactions for trip_id={trip_id} → settlement_id={settlement_id}")
    except Exception as e:
        print(f"⚠️ Failed to archive/clear settlement transactions — {e}")

    conn.close()
    print(f"🏁 Stay settlement completed successfully (ID={settlement_id})\n")
    return settlement_id





# ===============================================
# 🧮 ORCHESTRATOR (USED BY /settlement/{trip_id})
# ===============================================
def get_stay_settlement(trip_id: int, period="on_demand", record=False):
    start_date, end_date = determine_period(period)
    result = calculate_stay_settlement(trip_id, start_date, end_date, carry_forward=True)

    if record:
        settlement_id = record_stay_settlement(trip_id, result)
        result["recorded_id"] = settlement_id
    result["carry_forward_total"] = round(sum(f["previous_balance"] for f in result["families"]))
    return result
def record_carry_forward_log(trip_id: int, previous_settlement_id: int, new_settlement_id: int, families: list):
    """
    Logs carry-forward balances for each family from the previous to the new stay settlement.
    If this is the first settlement (no previous_settlement_id), creates a baseline log entry
    with zero delta so that the Carry Forward Log page always has a record.
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # ✅ Handle the very first settlement (no previous one)
        if not previous_settlement_id:
            print(f"🧾 [DEBUG] Trip {trip_id}: Creating baseline carry-forward log (first settlement, ID={new_settlement_id})")
            inserted_count = 0
            for fam in families:
                family_id = fam["family_id"]
                prev_balance = 0.0
                new_balance = float(fam.get("balance", 0.0))
                delta = new_balance  # Since prev_balance = 0

                cursor.execute("""
                    INSERT INTO stay_carry_forward_log (
                        trip_id, previous_settlement_id, new_settlement_id,
                        family_id, previous_balance, new_balance, delta, created_at
                    )
                    VALUES (%s, NULL, %s, %s, %s, %s, %s, NOW());
                """, (trip_id, new_settlement_id, family_id, prev_balance, new_balance, delta))
                inserted_count += 1

            conn.commit()
            conn.close()
            print(f"✅ [DEBUG] Baseline carry-forward log created — {inserted_count} rows inserted.")
            return

        # ✅ For normal carry-forward (has previous settlement)
        print(f"🧾 [DEBUG] Recording carry-forward log for trip {trip_id} (prev={previous_settlement_id}, new={new_settlement_id})")

        inserted_count = 0
        for fam in families:
            family_id = fam["family_id"]
            prev_balance = float(fam.get("previous_balance", 0.0))
            new_balance = float(fam.get("balance", 0.0))
            delta = round(new_balance - prev_balance)

            cursor.execute("""
                INSERT INTO stay_carry_forward_log (
                    trip_id, previous_settlement_id, new_settlement_id,
                    family_id, previous_balance, new_balance, delta, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW());
            """, (trip_id, previous_settlement_id, new_settlement_id, family_id, prev_balance, new_balance, delta))
            inserted_count += 1

        conn.commit()
        conn.close()
        print(f"✅ [DEBUG] Carry-forward log recorded successfully — {inserted_count} rows inserted into stay_carry_forward_log.")

    except Exception as e:
        import traceback
        print(f"⚠️ [DEBUG] Carry-forward log failed for trip {trip_id}: {e}")
        traceback.print_exc()










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
