from database import get_connection

def get_settlement(trip_id: int):
    conn = get_connection()
    conn.row_factory = lambda cursor, row: {col[0]: row[idx] for idx, col in enumerate(cursor.description)}
    cursor = conn.cursor()

    # --- Step 1: Get all families ---
    cursor.execute("""
        SELECT id, family_name, members_count
        FROM family_details
        WHERE trip_id = ?
    """, (trip_id,))
    families = cursor.fetchall()

    if not families:
        conn.close()
        return {"message": "No families found for this trip."}

    family_ids = [f["id"] for f in families]
    family_names = {f["id"]: f["family_name"] for f in families}
    family_members = {f["id"]: f["members_count"] for f in families}

    # --- Step 2: Get expenses ---
    cursor.execute("""
        SELECT payer_family_id, amount
        FROM expenses
        WHERE trip_id = ?
    """, (trip_id,))
    expenses = cursor.fetchall()

    expense_balance = {fid: 0.0 for fid in family_ids}
    total_expense = 0.0

    for e in expenses:
        payer_id = e["payer_family_id"]
        amt = e["amount"]
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
        WHERE trip_id = ?
    """, (trip_id,))
    advances = cursor.fetchall()

    advance_balance = {fid: 0.0 for fid in family_ids}

    for a in advances:
        payer_id = a["payer_family_id"]
        receiver_id = a["receiver_family_id"]
        amt = a["amount"]

        # apply only for families of this trip
        if payer_id in advance_balance:
            advance_balance[payer_id] += amt
        if receiver_id in advance_balance:
            advance_balance[receiver_id] -= amt

    # --- Step 5: Compute net balances ---
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
            "raw_balance": round(net, 2),  # store original balance
            "balance": round(net, 2)       # working balance (used in transactions)
        })

    # --- Step 6: Build settlement transactions ---
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

    # ✅ Step 7: Restore display balances (original raw values)
    for f in family_results:
        f["balance"] = f["raw_balance"]

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
    cursor = conn.cursor()

    # --- Trip info ---
    cursor.execute("SELECT * FROM trips WHERE id = ?", (trip_id,))
    trip = cursor.fetchone()
    if not trip:
        conn.close()
        return {"error": f"Trip with id {trip_id} not found"}

    trip_data = dict(trip)

    # --- Families ---
    cursor.execute("""
        SELECT id, family_name, members_count
        FROM family_details
        WHERE trip_id = ?
    """, (trip_id,))
    families = [dict(row) for row in cursor.fetchall()]

    # --- Expenses ---
    cursor.execute("""
        SELECT e.expense_name, e.amount, e.date, f.family_name AS payer
        FROM expenses e
        JOIN family_details f ON e.payer_family_id = f.id
        WHERE e.trip_id = ?
        ORDER BY e.date ASC, e.id ASC
    """, (trip_id,))
    expenses = [dict(row) for row in cursor.fetchall()]

    conn.close()

    # --- Settlement details ---
    settlement_data = get_settlement(trip_id)

    return {
        "trip": trip_data,
        "families": families,
        "expenses": expenses,
        "settlement": settlement_data
    }