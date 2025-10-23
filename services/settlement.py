from database import get_connection
import psycopg2.extras
from datetime import datetime, timedelta


def get_settlement(trip_id: int, start_date: str = None, end_date: str = None, record: bool = False):

    """
    Unified settlement logic for both TRIP and STAY modes.
    - TRIP → existing logic
    - STAY → supports date range, pro-rated costs, and recordable settlements
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # --- Step 0: Fetch trip mode and billing info ---
    cursor.execute("SELECT id, mode, billing_cycle, start_date FROM trips WHERE id = %s", (trip_id,))
    trip = cursor.fetchone()

    if not trip:
        cursor.close()
        conn.close()
        return {"error": f"Trip with id {trip_id} not found"}

    mode = trip.get("mode", "TRIP").upper()
    billing_cycle = trip.get("billing_cycle")
    trip_start = trip.get("start_date")

    # --- Step 1: Get all families ---
    cursor.execute("""
        SELECT id, family_name, members_count, join_date
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

    # --- Step 2: Get expenses (filtered for STAY) ---
    if mode == "STAY":
        # Determine default date range if not passed
        today = datetime.now().date()
        if not start_date:
            start_date = today.replace(day=1)  # start of current month
        else:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

        if not end_date:
            # end of current month
            next_month = (start_date.replace(day=28) + timedelta(days=4)).replace(day=1)
            end_date = next_month - timedelta(days=1)
        else:
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

        cursor.execute("""
            SELECT payer_family_id, amount, date
            FROM expenses
            WHERE trip_id = %s AND date BETWEEN %s AND %s
        """, (trip_id, start_date, end_date))
    else:
        # TRIP → all expenses
        cursor.execute("""
            SELECT payer_family_id, amount
            FROM expenses
            WHERE trip_id = %s
        """, (trip_id,))

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
    if mode == "STAY":
        # Adjust member counts based on join_date and period
        total_members = 0.0
        weighted_members = {}
        for f in families:
            join_date = f.get("join_date")
            if join_date:
                join_date = datetime.strptime(str(join_date), "%Y-%m-%d").date()
            else:
                join_date = start_date
            effective_start = max(join_date, start_date)
            active_days = (end_date - effective_start).days + 1
            month_days = (end_date - start_date).days + 1
            ratio = max(0, active_days / month_days)
            adjusted_members = f["members_count"] * ratio
            weighted_members[f["id"]] = adjusted_members
            total_members += adjusted_members
    else:
        weighted_members = family_members
        total_members = sum(family_members.values())

    per_head_cost = total_expense / total_members if total_members > 0 else 0.0
    expected_share = {fid: weighted_members[fid] * per_head_cost for fid in family_ids}

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

    # --- Step 5: Compute balances per family ---
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
            "effective_members": round(weighted_members[fid], 2),
            "total_spent": round(paid, 2),
            "balance": round(net, 2)
        })

    # --- Step 6: Calculate settlement transactions ---
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

    # --- Step 7: Optional recording for STAY settlements ---
    settlement_id = None
    if record and mode == "STAY":
        cursor.execute("""
            INSERT INTO stay_settlements (trip_id, period_start, period_end, total_expense, per_head_cost)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (trip_id, start_date, end_date, total_expense, per_head_cost))
        settlement_id = cursor.fetchone()["id"]

        for f in family_results:
            cursor.execute("""
                INSERT INTO stay_settlement_details
                    (settlement_id, family_id, family_name, members_count, total_spent, due_amount, balance)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                settlement_id, f["family_id"], f["family_name"],
                f["members_count"], f["total_spent"],
                expected_share[f["family_id"]], f["balance"]
            ))

    conn.commit()
    cursor.close()
    conn.close()

    return {
        "trip_id": trip_id,
        "mode": mode,
        "billing_cycle": billing_cycle,
        "period": {"start": str(start_date), "end": str(end_date)} if mode == "STAY" else None,
        "total_expense": round(total_expense, 2),
        "total_members": round(total_members, 2),
        "per_head_cost": round(per_head_cost, 2),
        "families": family_results,
        "transactions": transactions if transactions else "All accounts settled",
        "recorded_settlement_id": settlement_id
    }
