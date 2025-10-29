
from database import get_connection
import psycopg2.extras
from datetime import datetime
import json
from datetime import date, timedelta, datetime

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
    - per_head_cost by member count
    - Net = previous_carry_forward + (spent - due)
    - Adjusted = Net + (sum of active transactions impact)
      (payer +amt, receiver -amt)
    - Suggested settlements are computed from Adjusted balances
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # 1) total_expense, total_members, per_head_cost
    cursor.execute("""
        SELECT COALESCE(SUM(amount), 0) AS total_expense
        FROM expenses WHERE trip_id = %s;
    """, (trip_id,))
    total_expense = float(cursor.fetchone()["total_expense"] or 0)

    cursor.execute("""
        SELECT COALESCE(SUM(members_count), 0) AS total_members
        FROM family_details WHERE trip_id = %s;
    """, (trip_id,))
    total_members = int(cursor.fetchone()["total_members"] or 0) or 1
    per_head_cost = round(total_expense / total_members, 2)

    # 2) previous settlement (for carry-forward)
    cursor.execute("""
        SELECT id, period_end
        FROM stay_settlements
        WHERE trip_id = %s
        ORDER BY id DESC LIMIT 1;
    """, (trip_id,))
    prev = cursor.fetchone()
    prev_settlement_id = prev["id"] if prev else None
    prev_end_date = prev["period_end"] if prev else None

    # 3) carry-forward map
    previous_balance_map = {}
    if prev_settlement_id:
        cursor.execute("""
            SELECT family_id, balance
            FROM stay_settlement_details
            WHERE settlement_id = %s;
        """, (prev_settlement_id,))
        for row in cursor.fetchall():
            previous_balance_map[row["family_id"]] = float(row["balance"])

    # 4) families + raw spend/due/net
    cursor.execute("""
        SELECT id AS family_id, family_name, members_count
        FROM family_details WHERE trip_id = %s
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

        due = round(per_head_cost * int(f["members_count"]), 2)  # member-weighted
        prev_bal = float(previous_balance_map.get(fid, 0.0))

        # ✅ Correct logic:
        # The previous balance is already adjusted (settled value from last period)
        # So we add only the *new delta* (spent - due) on top of that carry-forward.
        if prev_settlement_id:
            net = round(spent - due, 2)
        else:
            net = round(prev_bal + (spent - due), 2)

        results.append({
            "family_id": fid,
            "family_name": f["family_name"],
            "members_count": int(f["members_count"]),
            "total_spent": float(spent),
            "due_amount": float(due),          # ← show this in UI “Share (₹)”
            "previous_balance": float(prev_bal),
            "balance": float(net),             # ← Net (₹)
        })
    print(f"🧮 [DEBUG] Computed family {f['family_name']}: spent={spent}, due={due}, prev={prev_bal}, net={net}")

    # 5) active transactions (with names) — used for ADJUSTED only, not for suggested
    cursor.execute("""
        SELECT 
            t.from_family_id,
            f1.family_name AS from_family,
            t.to_family_id,
            f2.family_name AS to_family,
            t.amount,
            t.transaction_date,
            t.remarks
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

    # 🩵 archived fallback (with names) – for UI tabs, not for suggested
    cursor.execute("""
        SELECT sta.from_family_id,
               f1.family_name AS from_family,
               sta.to_family_id,
               f2.family_name AS to_family,
               sta.amount,
               sta.transaction_date,
               sta.remarks,
               sta.settlement_id
        FROM settlement_transactions_archive sta
        LEFT JOIN family_details f1 ON sta.from_family_id = f1.id
        LEFT JOIN family_details f2 ON sta.to_family_id = f2.id
        WHERE sta.trip_id = %s
          AND sta.settlement_id = (
              SELECT MAX(id) FROM stay_settlements WHERE trip_id = %s
          )
        ORDER BY sta.id;
    """, (trip_id, trip_id))
    archived_fallback = cursor.fetchall()
    for txn in archived_fallback:
        txn["from"] = txn.get("from_family")
        txn["to"] = txn.get("to_family")

    # 6) build adjustments from active transactions only
    adjustments = {f["family_id"]: 0.0 for f in results}
    for txn in active_txns:
        f_from = txn["from_family_id"]
        f_to = txn["to_family_id"]
        amt = float(txn["amount"])
        # payer owes less (moves toward zero)
        adjustments[f_from] = adjustments.get(f_from, 0.0) + amt
        # receiver is owed less (moves toward zero)
        adjustments[f_to] = adjustments.get(f_to, 0.0) - amt
    print("🔧 Adjustments applied (payer pays = balance increases):")
    for f in results:
        fid = f["family_id"]
        adj = adjustments.get(fid, 0.0)
        print(f"▶ {f['family_name']}: Net={f['balance']} + Adj({adj:+.2f}) = Adjusted={f['balance'] + adj:.2f}")
    # 7) adjusted balances
    for f in results:
        fid = f["family_id"]
        adj = float(adjustments.get(fid, 0.0))
        f["adjusted_balance"] = round(f["balance"] + adj, 2)

    # 8) compute suggested settlements from adjusted balances (NOT from records table)
    #    debtors negative, creditors positive
    creditors = [{
        "family_id": f["family_id"],
        "family_name": f["family_name"],
        "bal": f["adjusted_balance"]
    } for f in results if f["adjusted_balance"] > 0.01]

    debtors = [{
        "family_id": f["family_id"],
        "family_name": f["family_name"],
        "bal": f["adjusted_balance"]
    } for f in results if f["adjusted_balance"] < -0.01]

    # Sort optional (largest first)
    creditors.sort(key=lambda x: -x["bal"])
    debtors.sort(key=lambda x: x["bal"])  # most negative first

    suggested = []
    ci, di = 0, 0
    while di < len(debtors) and ci < len(creditors):
        pay_amt = min(creditors[ci]["bal"], -debtors[di]["bal"])
        if pay_amt <= 0:
            break
        suggested.append({
            "from": debtors[di]["family_name"],
            "to": creditors[ci]["family_name"],
            "amount": round(pay_amt, 2)
        })
        creditors[ci]["bal"] = round(creditors[ci]["bal"] - pay_amt, 2)
        debtors[di]["bal"] = round(debtors[di]["bal"] + pay_amt, 2)
        if abs(creditors[ci]["bal"]) < 0.01:
            ci += 1
        if abs(debtors[di]["bal"]) < 0.01:
            di += 1

    # 9) period
    period_start = prev_end_date if prev_end_date else datetime.utcnow().date()
    period_end = datetime.utcnow().date()

    conn.close()

    # normalize types for JSON
    for f in results:
        f["family_id"] = int(f["family_id"])
        f["members_count"] = int(f["members_count"])
        f["total_spent"] = float(f["total_spent"])
        f["due_amount"] = float(f["due_amount"])
        f["balance"] = float(f["balance"])
        f["adjusted_balance"] = float(f.get("adjusted_balance", f["balance"]))

    return {
        "period_start": period_start,
        "period_end": period_end,
        "total_expense": round(total_expense, 2),
        "total_members": total_members,
        "per_head_cost": per_head_cost,
        "families": results,
        # carry-forward info
        "carry_forward": bool(previous_balance_map),
        "carry_forward_breakdown": [
            {"family_id": fid, "previous_balance": bal}
            for fid, bal in previous_balance_map.items()
        ],
        "previous_settlement_id": prev_settlement_id,
        # 🔹 NEW: keep active txns and archived fallback for your bottom tabs
        "active_transactions": active_txns,
        "archived_transactions": archived_fallback,
        # 🔹 NEW: suggested settlements for “Who pays whom”
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

    print(f"🧾 Finalizing stay settlement for trip {trip_id}...")

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

    # ✅ Only skip if the *same settlement* was saved seconds ago (not based on prev_id)
    if existing and existing[1]:
        created_time = existing[1]
        now = datetime.now(timezone.utc)
        seconds_since = (now - created_time).total_seconds()
        if seconds_since < 5:
            print(f"⚠️ Skipping immediate re-finalization for trip {trip_id} "
                  f"(last settlement {seconds_since:.1f}s ago)")
            conn.close()
            return last_id

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
        # ✅ Store *net* balance (not adjusted) to carry forward real financial position
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
            round(balance_value, 2)
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
    result["carry_forward_total"] = round(sum(f["previous_balance"] for f in result["families"]), 2)
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
            delta = round(new_balance - prev_balance, 2)

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

    # 🔍 0️⃣ Check for duplicate prevention (with safer logic)
    cursor.execute("""
        SELECT id FROM stay_settlements 
        WHERE trip_id = %s
        ORDER BY id DESC LIMIT 1;
    """, (trip_id,))
    existing = cursor.fetchone()

    prev_id = result.get("previous_settlement_id")
    print(f"🔍 Checking duplicate prevention: prev_id={prev_id}, last_settlement_in_db={existing[0] if existing else None}")

    # ✅ Only skip if BOTH exist and are identical
    # ✅ Only skip if the *same settlement* was just saved (within a few seconds)
    if existing and len(existing) > 1 and existing[1] is not None:
        from datetime import datetime, timezone
        created_time = existing[1]
        now = datetime.now(timezone.utc)
        seconds_since = (now - created_time).total_seconds()
        if seconds_since < 5:
            print(f"⚠️ Skipping immediate re-finalization for trip {trip_id} "
                f"(last settlement {seconds_since:.1f}s ago)")
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

    # 2️⃣ Insert family-level details (using adjusted balances)
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
        print(f"🧾 Calling record_carry_forward_log(prev={previous_settlement_id}, new={settlement_id})")
        record_carry_forward_log(trip_id, previous_settlement_id, settlement_id, result["families"])

        # ✅ Add quick DB verification print
        cursor.execute("SELECT COUNT(*) FROM stay_carry_forward_log WHERE trip_id = %s;", (trip_id,))
        log_count = cursor.fetchone()[0]
        print(f"🔍 [DEBUG] stay_carry_forward_log now has {log_count} rows for trip {trip_id}.")

    except Exception as e:
        print(f"⚠️ Carry-forward log skipped due to error: {e}")

    # 4️⃣ Archive settlement transactions
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
