# settlement.py

from datetime import date, timedelta, datetime, timezone
import json
import psycopg2.extras

from database import get_connection


# =========================
# Utility: Period helpers
# =========================
def determine_period(period_type: str):
    """Returns (start_date, end_date) for the given period type."""
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


def get_settlement(trip_id: int, start_date: str = None, end_date: str = None, record: bool = False):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Optional date filter (for future use, TRIP uses full trip normally)
    date_filter = ""
    if start_date and end_date:
        date_filter = "AND e.date BETWEEN %s AND %s"
        date_params = (trip_id, start_date, end_date)
    else:
        date_params = (trip_id,)

    # --- Step 1: Get families ---
    cursor.execute("""
        SELECT id, family_name, members_count
        FROM family_details
        WHERE trip_id = %s
    """, (trip_id,))
    families_rows = cursor.fetchall()

    if not families_rows:
        cursor.close()
        conn.close()
        return {"message": "No families found for this trip."}

    family_ids = [f["id"] for f in families_rows]
    family_names = {f["id"]: f["family_name"] for f in families_rows}
    family_members = {f["id"]: f["members_count"] for f in families_rows}

    # --- Step 2: Expenses (optionally filtered) ---
    cursor.execute(f"""
        SELECT payer_family_id, amount
        FROM expenses e
        WHERE trip_id = %s {date_filter}
    """, date_params)
    expenses = cursor.fetchall()

    expense_balance = {fid: 0.0 for fid in family_ids}
    total_expense = 0.0

    for e in expenses:
        payer_id = e["payer_family_id"]
        amt = float(e["amount"])
        if payer_id in expense_balance:
            expense_balance[payer_id] += amt
            total_expense += amt

    # --- Step 3: Per-head, expected share ---
    total_members = sum(family_members.values())
    per_head_cost = total_expense / total_members if total_members > 0 else 0.0
    expected_share = {fid: family_members[fid] * per_head_cost for fid in family_ids}

    # --- Step 4: Advances (giver = +, taker = -) ---
    cursor.execute("""
        SELECT payer_family_id, receiver_family_id, amount
        FROM advances
        WHERE trip_id = %s
    """, (trip_id,))
    advances_rows = cursor.fetchall()

    advance_balance = {fid: 0.0 for fid in family_ids}
    for a in advances_rows:
        payer_id = a["payer_family_id"]
        receiver_id = a["receiver_family_id"]
        amt = float(a["amount"])
        if payer_id in advance_balance:
            advance_balance[payer_id] += amt   # gave → credit
        if receiver_id in advance_balance:
            advance_balance[receiver_id] -= amt  # took → debit

    # --- Step 5: Raw balances (before settlement payments) ---
    family_results = []
    for fid in family_ids:
        paid = float(expense_balance.get(fid, 0.0))
        owed = float(expected_share.get(fid, 0.0))
        adv  = float(advance_balance.get(fid, 0.0))
        net  = paid - owed + adv   # RAW/NET

        family_results.append({
            "family_id": fid,
            "family_name": family_names[fid],
            "members_count": family_members[fid],
            "total_spent": paid,       # will round later for output
            "raw_balance": net,        # before settlement payments
            "balance": net,            # used internally, will keep raw
            # adjusted_balance will be added after applying settlement txns
        })

    # --- Step 5B: Apply Settlement Transactions (TRIP mode adjustments) ---
    cursor.execute("""
        SELECT from_family_id, to_family_id, amount
        FROM settlement_transactions
        WHERE trip_id = %s
    """, (trip_id,))
    txn_rows = cursor.fetchall()

    txn_adjust = {fid: 0.0 for fid in family_ids}
    for t in txn_rows:
        f_from = t["from_family_id"]
        f_to   = t["to_family_id"]
        amt    = float(t["amount"])
        # from pays → owes less → balance moves toward zero (increase)
        txn_adjust[f_from] = txn_adjust.get(f_from, 0.0) + amt
        # to receives → should receive less → balance moves toward zero (decrease)
        txn_adjust[f_to] = txn_adjust.get(f_to, 0.0) - amt

    # apply adjustments
    for fam in family_results:
        fid = fam["family_id"]
        adj = txn_adjust.get(fid, 0.0)
        fam["adjusted_balance"] = fam["balance"] + adj  # balance is net

    # --- Step 6: Suggested settlements derived from adjusted_balance ---
    # work on copies so we don't mutate family_results
    debtors = [
        {
            "family_name": f["family_name"],
            "bal": abs(f["adjusted_balance"])
        }
        for f in family_results
        if f["adjusted_balance"] < -0.5  # owes
    ]
    creditors = [
        {
            "family_name": f["family_name"],
            "bal": f["adjusted_balance"]
        }
        for f in family_results
        if f["adjusted_balance"] > 0.5   # to receive
    ]

    transactions = []
    for d in debtors:
        owed = d["bal"]
        for c in creditors:
            if owed <= 0:
                break
            if c["bal"] <= 0:
                continue
            payment = min(owed, c["bal"])
            if payment <= 0:
                continue
            transactions.append({
                "from": d["family_name"],
                "to": c["family_name"],
                "amount": round(payment)
            })
            owed     -= payment
            c["bal"] -= payment
    # --- Step 7: Fetch settlement transactions (TRIP mode)
    cursor.execute("""
        SELECT t.id, 
            f1.family_name AS from_family,
            f2.family_name AS to_family,
            t.amount, 
            t.transaction_date,
            t.remarks
        FROM settlement_transactions t
        LEFT JOIN family_details f1 ON t.from_family_id = f1.id
        LEFT JOIN family_details f2 ON t.to_family_id = f2.id
        WHERE t.trip_id = %s
        ORDER BY t.id DESC
    """, (trip_id,))
    active_transactions = cursor.fetchall()

    # --- Step 7: Round values for output only ---
    for f in family_results:
        f["total_spent"]      = round(f["total_spent"])
        f["raw_balance"]      = round(f["raw_balance"])
        f["balance"]          = round(f["raw_balance"])  # keep raw as base
        f["adjusted_balance"] = round(f.get("adjusted_balance", f["raw_balance"]))

    cursor.close()
    conn.close()

    return {
        "total_expense": round(total_expense),
        "total_members": total_members,
        "per_head_cost": round(per_head_cost),
        "families": family_results,
        # keep legacy "transactions" for compatibility (trip-level suggestion)
        "transactions": transactions if transactions else "All accounts settled",
        # also expose a proper list for new UIs if you want:
        "suggested": transactions,
        "active_transactions": active_transactions
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
# =========================
# Fetch last STAY settlement
# =========================
def get_last_stay_settlement(trip_id: int):
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(
        """
        SELECT id, period_start, period_end, created_at
        FROM stay_settlements
        WHERE trip_id = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (trip_id,),
    )
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result


def _build_expense_time_filter(cursor, prev_end_date, prev_created_at):
    """
    Returns (clause_sql, params_tuple) to append in WHERE for expenses.
    Prefers expenses.created_at > prev_created_at (exact), else falls back to date >= prev_end_date.
    If no previous settlement, returns ("", ()).
    """
    if not prev_end_date and not prev_created_at:
        return ("", ())

    # Check if expenses.created_at exists
    cursor.execute("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'expenses' AND column_name = 'created_at'
        LIMIT 1;
    """)
    has_created_at = cursor.fetchone() is not None

    if has_created_at and prev_created_at:
        # strict: after instant of last finalize
        return (" AND e.created_at > %s ", (prev_created_at,))
    else:
        # fall back: parse text date safely
        return (" AND to_date(e.date, 'YYYY-MM-DD') >= %s ", (prev_end_date or date.today(),))


# =============================================
# Core: Calculate STAY settlement (current view)
# =============================================
def calculate_stay_settlement(trip_id: int):
    """
    STAY mode settlement:
    - Per-head cost based on total members
    - Net = previous_carry_forward + (period_spent - period_due)
    - Adjusted = Net + sum(active settlement transactions only)
    - Suggested settlements derived from ADJUSTED
    - Internals use floats; round only for output & suggestions
    - IMPORTANT: Expenses are limited to the current period
                 (i.e., only after the last finalized settlement).
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # 1) Previous settlement (for carry-forward & period boundary)
    cursor.execute("""
        SELECT id, period_end, created_at
        FROM stay_settlements
        WHERE trip_id = %s
        ORDER BY id DESC
        LIMIT 1;
    """, (trip_id,))
    prev = cursor.fetchone()
    prev_settlement_id = prev["id"] if prev else None
    prev_end_date = prev["period_end"] if prev else None
    prev_created_at = prev["created_at"] if prev and "created_at" in prev else None

    time_where_sql, time_where_params = _build_expense_time_filter(cursor, prev_end_date, prev_created_at)

    # 2) total_expense (PERIOD ONLY), total_members, per-head cost (float)
    cursor.execute(
        f"""SELECT COALESCE(SUM(e.amount), 0) AS total_expense
            FROM expenses e
            WHERE e.trip_id = %s {time_where_sql};
        """,
        (trip_id, *time_where_params) if time_where_params else (trip_id,)
    )
    total_expense = float(cursor.fetchone()["total_expense"] or 0.0)

    cursor.execute(
        "SELECT COALESCE(SUM(members_count), 0) AS total_members FROM family_details WHERE trip_id = %s;",
        (trip_id,),
    )
    total_members = int(cursor.fetchone()["total_members"] or 0) or 1
    per_head_cost = total_expense / total_members

    # 3) Carry-forward map — from the latest finalized settlement (ADJUSTED preferred)
    previous_balance_map = {}
    if prev_settlement_id:
        with get_connection() as cf_conn:
            cf_cur = cf_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cf_cur.execute(
                """
                SELECT ssd.family_id,
                       COALESCE(ssd.adjusted_balance, ssd.balance, 0.0) AS carry_forward_balance
                FROM stay_settlement_details ssd
                WHERE ssd.settlement_id = %s;
                """,
                (prev_settlement_id,),
            )
            for row in cf_cur.fetchall():
                previous_balance_map[row["family_id"]] = float(row["carry_forward_balance"] or 0.0)
            cf_cur.close()

    print(f"🧾 [DEBUG] Loaded carry-forward map for trip {trip_id}: {previous_balance_map}")

    # 4) Compute family balances (Net) using only PERIOD expenses
    cursor.execute(
        """
        SELECT id AS family_id, family_name, members_count
        FROM family_details
        WHERE trip_id = %s
        ORDER BY id;
        """,
        (trip_id,),
    )
    families = cursor.fetchall()

    results = []
    for f in families:
        fid = f["family_id"]
        # period-spent for this family
        cursor.execute(
            f"""
            SELECT COALESCE(SUM(e.amount), 0) AS spent
            FROM expenses e
            WHERE e.trip_id = %s AND e.payer_family_id = %s {time_where_sql};
            """,
            (trip_id, fid, *time_where_params) if time_where_params else (trip_id, fid)
        )
        spent = float(cursor.fetchone()["spent"] or 0.0)
        due = per_head_cost * int(f["members_count"])
        prev_bal = previous_balance_map.get(fid, 0.0)

        net = prev_bal + (spent - due)

        results.append(
            {
                "family_id": fid,
                "family_name": f["family_name"],
                "members_count": int(f["members_count"]),
                "total_spent": spent,
                "due_amount": due,
                "previous_balance": prev_bal,
                "balance": net,  # NET (before payments)
            }
        )
        print(
            f"🧮 [DEBUG] Family {f['family_name']}: spent={spent:.2f}, due={due:.2f}, prev={prev_bal:.2f}, net={net:.2f}"
        )

    # 5) Load transactions for UI tabs
    #    - active = used in ADJUSTED (current period)
    #    - archived = last settlement's transactions (for UI only; NOT re-applied)
    cursor.execute(
        """
        SELECT t.id, t.from_family_id,
               f1.family_name AS from_family,
               t.to_family_id,
               f2.family_name AS to_family,
               t.amount, t.transaction_date, t.remarks
        FROM settlement_transactions t
        LEFT JOIN family_details f1 ON t.from_family_id = f1.id
        LEFT JOIN family_details f2 ON t.to_family_id = f2.id
        WHERE t.trip_id = %s
        ORDER BY t.id;
        """,
        (trip_id,),
    )
    active_txns = cursor.fetchall()
    for txn in active_txns:
        txn["from"] = txn.get("from_family")
        txn["to"] = txn.get("to_family")

    cursor.execute(
        """
        SELECT sta.id, sta.from_family_id,
               f1.family_name AS from_family,
               sta.to_family_id,
               f2.family_name AS to_family,
               sta.amount, sta.transaction_date, sta.remarks, sta.settlement_id
        FROM settlement_transactions_archive sta
        LEFT JOIN family_details f1 ON sta.from_family_id = f1.id
        LEFT JOIN family_details f2 ON sta.to_family_id = f2.id
        WHERE sta.trip_id = %s
          AND sta.settlement_id = (SELECT MAX(id) FROM stay_settlements WHERE trip_id = %s)
        ORDER BY sta.id;
        """,
        (trip_id, trip_id),
    )
    archived_txns = cursor.fetchall()
    for txn in archived_txns:
        txn["from"] = txn.get("from_family")
        txn["to"] = txn.get("to_family")

    # 6) Apply adjustments from ACTIVE transactions ONLY
    #    ("from" pays → +amt; "to" receives → -amt)
    adjustments = {f["family_id"]: 0.0 for f in results}
    for txn in active_txns:
        f_from, f_to = txn["from_family_id"], txn["to_family_id"]
        amt = float(txn["amount"])
        adjustments[f_from] = adjustments.get(f_from, 0.0) + amt
        adjustments[f_to] = adjustments.get(f_to, 0.0) - amt

    print("🔧 Adjustments applied (ACTIVE transactions only):")
    for f in results:
        fid = f["family_id"]
        adj = adjustments.get(fid, 0.0)
        adjusted = f["balance"] + adj
        f["adjusted_balance"] = adjusted
        print(
            f"▶ {f['family_name']}: Net={f['balance']:.2f} + Adj({adj:+.2f}) = Adjusted={adjusted:.2f}"
        )

    # 6b) Ensure the adjusted balances sum to exactly 0.00 (guard tiny drift)
    total_adj = sum(f["adjusted_balance"] for f in results)
    if abs(total_adj) > 0.01:
        # apply correction to the largest absolute adjusted so the vector sum is 0
        target = max(results, key=lambda x: abs(x["adjusted_balance"]))
        target["adjusted_balance"] -= total_adj
        print(
            f"🔧 Final correction {(-total_adj):+.2f} applied to {target['family_name']} (ensured total=0.00)"
        )

    # 7) Suggested settlements (from adjusted)
    creditors = [
        {"family_name": f["family_name"], "bal": f["adjusted_balance"]}
        for f in results
        if f["adjusted_balance"] > 0.01
    ]
    debtors = [
        {"family_name": f["family_name"], "bal": f["adjusted_balance"]}
        for f in results
        if f["adjusted_balance"] < -0.01
    ]
    creditors.sort(key=lambda x: -x["bal"])
    debtors.sort(key=lambda x: x["bal"])

    suggested = []
    ci = di = 0
    while ci < len(creditors) and di < len(debtors):
        c_bal = round(creditors[ci]["bal"], 2)
        d_bal = round(debtors[di]["bal"], 2)
        if c_bal < 0.01:
            ci += 1
            continue
        if d_bal > -0.01:
            di += 1
            continue
        pay_amt = min(c_bal, -d_bal)
        if pay_amt > 0.01:
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

    # 8) Period & finalize output (round for UI only)
    period_start = (prev_end_date + timedelta(days=1)) if prev_end_date else datetime.utcnow().date()
    period_end = datetime.utcnow().date()
    conn.close()

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
        "archived_transactions": archived_txns,  # for UI only
        "suggested": suggested,
    }



# ======================================
# Finalize & record STAY settlement
# ======================================
def record_stay_settlement(trip_id: int, result: dict):
    """
    Finalizes and records the stay settlement.
    - Saves summary and family-level balances (both net & adjusted)
    - Archives active transactions and clears them
    - Creates idempotent carry-forward log
    - Prevents accidental duplicate re-finalization (<5s)
    - If everything is already adjusted to zero, records a zero-closure settlement
    """
    conn = get_connection()
    cursor = conn.cursor()

    print(f"🧾 Finalizing stay settlement for trip {trip_id}...")

    # prevent immediate re-finalization within 5 seconds
    cursor.execute(
        """
        SELECT id, created_at
        FROM stay_settlements
        WHERE trip_id = %s
        ORDER BY id DESC LIMIT 1;
        """,
        (trip_id,),
    )
    existing = cursor.fetchone()
    prev_id = result.get("previous_settlement_id")
    last_id = existing[0] if existing else None
    print(
        f"🔍 Checking duplicate prevention: prev_id={prev_id}, last_settlement_in_db={last_id}"
    )

    if existing and existing[1]:
        created_time = existing[1].replace(tzinfo=None)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        seconds_since = (now - created_time).total_seconds()
        if seconds_since < 5:
            print(
                f"⚠️ Skipping immediate re-finalization for trip {trip_id} "
                f"(last settlement {seconds_since:.1f}s ago)"
            )
            conn.close()
            return last_id

    # Always allow recording; if all balances are ~0, treat as closure entry
    all_balances = [round(f.get("adjusted_balance", f["balance"]), 2) for f in result["families"]]
    if all(abs(b) < 0.01 for b in all_balances):
        print(
            f"ℹ️ All balances are settled for trip {trip_id}, recording zero-balance closure entry."
        )

    try:
        # 1) summary row
        cursor.execute(
            """
            INSERT INTO stay_settlements (
                trip_id, total_expense, total_members, per_head_cost,
                period_start, period_end, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            RETURNING id;
            """,
            (
                trip_id,
                result["total_expense"],
                result["total_members"],
                result["per_head_cost"],
                result["period_start"],
                result["period_end"],
            ),
        )
        settlement_id = cursor.fetchone()[0]
        print(f"✅ Settlement summary saved (ID={settlement_id})")

        # 2) details — store both net & adjusted
        for f in result["families"]:
            net_balance = round(float(f.get("balance", 0.0)), 2)
            adjusted_balance = round(float(f.get("adjusted_balance", net_balance)), 2)
            if abs(adjusted_balance) < 0.01:
                adjusted_balance = 0.0
            if abs(net_balance) < 0.01:
                net_balance = 0.0
            cursor.execute(
                """
                INSERT INTO stay_settlement_details (
                    settlement_id, family_id, balance, adjusted_balance
                ) VALUES (%s, %s, %s, %s);
                """,
                (settlement_id, f["family_id"], net_balance, adjusted_balance),
            )
        print("✅ Family-level settlement details saved.")
        conn.commit()      # commit summary + details before logging
        print(f"✅ Settlement summary & details committed (ID={settlement_id})")

        # 3) carry-forward log (idempotent and correct ordering)
        print(f"🧾 Calling record_carry_forward_log(prev={prev_id}, new={settlement_id})")
        record_carry_forward_log(
            prev_settlement_id=prev_id,
            new_settlement_id=settlement_id,
            trip_id=trip_id,
            cursor=cursor,
        )

        # 3b) settlement history snapshot (safe no-op if table missing)
        carry_forward_map = {}
        cursor.execute("""
            SELECT family_id, adjusted_balance
            FROM stay_settlement_details
            WHERE settlement_id = %s;
        """, (settlement_id,))
        for row in cursor.fetchall():
            fid, adj = row[0], float(row[1] or 0.0)
            carry_forward_map[fid] = adj

        record_settlement_snapshot(
            trip_id=trip_id,
            prev_settlement_id=prev_id,
            new_settlement_id=settlement_id,
            mode="STAY",
            result_data=result,
            carry_forward_map=carry_forward_map
        )

        # 4) archive & clear active settlement transactions
        cursor.execute(
            """
            INSERT INTO settlement_transactions_archive (
                trip_id, from_family_id, to_family_id, amount, transaction_date, remarks, settlement_id
            )
            SELECT trip_id, from_family_id, to_family_id, amount, transaction_date, remarks, %s
            FROM settlement_transactions
            WHERE trip_id = %s;
            """,
            (settlement_id, trip_id),
        )
        cursor.execute("DELETE FROM settlement_transactions WHERE trip_id = %s;", (trip_id,))
        print(
            f"📦 Archived and cleared settlement transactions for trip_id={trip_id} → settlement_id={settlement_id}"
        )

        conn.commit()
        print(f"🏁 Stay settlement completed successfully (ID={settlement_id})")
        return settlement_id

    except Exception as e:
        conn.rollback()
        import traceback
        print(f"❌ Error while recording stay settlement: {e}")
        traceback.print_exc()
        raise

    finally:
        conn.close()


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


# ==========================================
# Carry-forward Log (idempotent, DB-driven)
# ==========================================
def record_carry_forward_log(prev_settlement_id, new_settlement_id, trip_id, cursor):
    """
    Creates carry-forward log rows comparing previous vs new settlement detail rows.
    - If no previous: baseline entries from 'new' with prev=0
    - If previous: delta = new.adjusted - prev.adjusted (fallback to balance)
    - Idempotent for (trip_id, new_settlement_id)
    """
    # skip if already logged
    cursor.execute(
        """
        SELECT COUNT(*) FROM stay_carry_forward_log
        WHERE trip_id = %s AND new_settlement_id = %s;
        """,
        (trip_id, new_settlement_id),
    )
    if cursor.fetchone()[0] > 0:
        print(
            f"⚠️ [DEBUG] Carry-forward log already exists for trip {trip_id}, settlement {new_settlement_id} — skipping."
        )
        return

    # first settlement → baseline
    if not prev_settlement_id:
        print(
            f"🧾 [DEBUG] Trip {trip_id}: Creating baseline carry-forward log (first settlement, ID={new_settlement_id})"
        )
        cursor.execute(
            """
            INSERT INTO stay_carry_forward_log (
                trip_id, previous_settlement_id, new_settlement_id,
                family_id, previous_balance, new_balance, delta, created_at
            )
            SELECT
                %s, NULL, %s,
                ssd.family_id,
                0.0,
                COALESCE(ssd.adjusted_balance, ssd.balance, 0.0),
                COALESCE(ssd.adjusted_balance, ssd.balance, 0.0),
                NOW()
            FROM stay_settlement_details ssd
            WHERE ssd.settlement_id = %s;
            """,
            (trip_id, new_settlement_id, new_settlement_id),
        )
        print(
            f"✅ [DEBUG] Baseline carry-forward log created — {cursor.rowcount} rows inserted."
        )
        return

    # normal prev → new
    print(
        f"🧾 [DEBUG] Recording carry-forward log for trip {trip_id} (prev={prev_settlement_id}, new={new_settlement_id})"
    )
    cursor.execute(
        """
        INSERT INTO stay_carry_forward_log (
            trip_id, previous_settlement_id, new_settlement_id,
            family_id, previous_balance, new_balance, delta, created_at
        )
        SELECT
            %s, %s, %s,
            newd.family_id,
            COALESCE(prevd.adjusted_balance, prevd.balance, 0.0) AS previous_balance,
            COALESCE(newd.adjusted_balance, newd.balance, 0.0)   AS new_balance,
            COALESCE(newd.adjusted_balance, newd.balance, 0.0)
          - COALESCE(prevd.adjusted_balance, prevd.balance, 0.0) AS delta,
            NOW()
        FROM stay_settlement_details newd
        LEFT JOIN stay_settlement_details prevd
          ON prevd.family_id = newd.family_id
         AND prevd.settlement_id = %s
        WHERE newd.settlement_id = %s;
        """,
        (trip_id, prev_settlement_id, new_settlement_id, prev_settlement_id, new_settlement_id),
    )
    print(
        f"✅ [DEBUG] Carry-forward log recorded successfully — {cursor.rowcount} rows inserted into stay_carry_forward_log."
    )
# ==========================================
# Stay Settlement → History Snapshot Writer
# ==========================================
import json
from decimal import Decimal
from datetime import datetime, date

# ==========================================
# Stay Settlement → History Snapshot Writer (Decimal + Datetime safe)
# ==========================================
def record_settlement_snapshot(
    trip_id: int,
    prev_settlement_id: int,
    new_settlement_id: int,
    mode: str,
    result_data: dict,
    carry_forward_map: dict,
    finalized_by: str = None,
):
    """
    Inserts a snapshot of each finalized stay settlement into stay_settlement_history.
    Includes full metadata such as period, trip type, finalized user, and delta summary.
    """

    conn = get_connection()
    cursor = conn.cursor()

    def _convert(obj):
        """Recursively converts Decimal → float and datetime/date → str for JSON serialization."""
        if isinstance(obj, list):
            return [_convert(x) for x in obj]
        elif isinstance(obj, dict):
            return {k: _convert(v) for k, v in obj.items()}
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return obj

    try:
        # 🔍 Skip duplicate entry
        cursor.execute(
            """
            SELECT 1 FROM stay_settlement_history
            WHERE trip_id = %s AND new_settlement_id = %s;
            """,
            (trip_id, new_settlement_id),
        )
        if cursor.fetchone():
            print(
                f"⚠️ [DEBUG] Settlement history already recorded for trip {trip_id}, settlement {new_settlement_id} — skipping."
            )
            conn.close()
            return

        # ============================
        # 1️⃣ Extract metadata
        # ============================
        period_start = result_data.get("period_start")
        period_end = result_data.get("period_end")
        trip_type = mode.upper() if mode else "STAY"

        # ============================
        # 2️⃣ Compute delta summary (prev vs new)
        # ============================
        net_delta_summary = []
        if prev_settlement_id:
            cursor.execute(
                """
                SELECT ssd.family_id,
                       COALESCE(ssd.adjusted_balance, ssd.balance, 0.0) AS prev_balance
                FROM stay_settlement_details ssd
                WHERE ssd.settlement_id = %s;
                """,
                (prev_settlement_id,),
            )
            prev_balances = {r[0]: float(r[1] or 0.0) for r in cursor.fetchall()}
        else:
            prev_balances = {}

        for fid, new_bal in carry_forward_map.items():
            old_bal = prev_balances.get(fid, 0.0)
            net_delta_summary.append(
                {
                    "family_id": fid,
                    "previous_balance": old_bal,
                    "new_balance": float(new_bal),
                    "delta": round(float(new_bal) - old_bal, 2),
                }
            )

        # ============================
        # 3️⃣ Prepare safe JSON content
        # ============================
        family_summary = _convert(result_data.get("families", []))
        suggested_settlements = _convert(result_data.get("suggested", []))
        settlement_transactions = _convert(result_data.get("active_transactions", []))
        carry_forward_data = _convert(
            [{"family_id": fid, "balance": bal} for fid, bal in carry_forward_map.items()]
        )
        net_delta_summary = _convert(net_delta_summary)

        # ============================
        # 4️⃣ Insert snapshot
        # ============================
        cursor.execute(
            """
            INSERT INTO stay_settlement_history (
                trip_id,
                prev_settlement_id,
                new_settlement_id,
                mode,
                trip_type,
                period_start,
                period_end,
                total_expense,
                total_members,
                per_head_cost,
                finalized_by,
                family_summary,
                suggested_settlements,
                settlement_transactions,
                carry_forward_data,
                net_delta_summary,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
                    NOW());
            """,
            (
                trip_id,
                prev_settlement_id,
                new_settlement_id,
                mode,
                trip_type,
                period_start,
                period_end,
                float(result_data.get("total_expense", 0)),
                int(result_data.get("total_members", 0)),
                float(result_data.get("per_head_cost", 0.0)),
                finalized_by,
                json.dumps(family_summary),
                json.dumps(suggested_settlements),
                json.dumps(settlement_transactions),
                json.dumps(carry_forward_data),
                json.dumps(net_delta_summary),
            ),
        )

        conn.commit()
        print(f"✅ Stay settlement snapshot (metadata) saved for trip {trip_id} (settlement_id={new_settlement_id})")

    except Exception as e:
        conn.rollback()
        import traceback
        print(f"❌ Error while recording stay settlement snapshot: {e}")
        traceback.print_exc()
    finally:
        conn.close()
