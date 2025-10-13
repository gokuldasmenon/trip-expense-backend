from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from models import TripIn, FamilyIn, ExpenseIn, FamilyUpdate, ExpenseUpdate
from database import initialize_database
from services import trips, families, expenses, advances, settlement
from fastapi.middleware.cors import CORSMiddleware
import random, string
app = FastAPI(title="Expense Tracker API")
# ✅ Enable CORS for Flutter app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # you can restrict this later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ✅ Initialize database
@app.on_event("startup")
def on_startup():
    initialize_database()

# ✅ Health Check
@app.get("/")
def home():
    return {"message": "Expense Tracker Backend Running"}

# =========================================================
# 🚀 TRIPS
# =========================================================

@app.post("/create_trip")
def create_trip(trip: TripIn):
    from services.trips import generate_access_code
    code = generate_access_code()
    return trips.add_trip(trip.name, trip.start_date, trip.trip_type, code)

@app.get("/join_trip/{access_code}")
def join_trip(access_code: str):
    trip = trips.get_trip_by_code(access_code)
    if not trip:
        raise HTTPException(status_code=404, detail="Invalid access code.")
    return {"message": "Trip joined successfully", "trip": trip}



@app.get("/trips")
def get_active_trips():
    return trips.get_all_trips()

@app.get("/archived_trips")
def get_archived_trips():
    return trips.get_archived_trips()

@app.put("/trips/archive/{trip_id}")
def archive_trip(trip_id: int):
    return trips.archive_trip(trip_id)

@app.put("/trips/restore/{trip_id}")
def restore_trip(trip_id: int):
    return trips.restore_trip(trip_id)

@app.delete("/trips/{trip_id}")
def delete_trip(trip_id: int):
    return trips.delete_trip(trip_id)

# =========================================================
# 👨‍👩‍👧 FAMILIES
# =========================================================

@app.post("/add_family")
def add_family(family: FamilyIn):
    return families.add_family(family.trip_id, family.family_name, family.members_count)

@app.get("/families/{trip_id}")
def get_families(trip_id: int):
    return families.get_families(trip_id)

@app.put("/update_family/{family_id}")
def update_family(family_id: int, family: FamilyUpdate):
    return families.update_family(family_id, family.family_name, family.members_count)

@app.delete("/delete_family/{family_id}")
def delete_family(family_id: int):
    return families.delete_family(family_id)

# =========================================================
# 💰 EXPENSES
# =========================================================

@app.post("/add_expense")
def add_expense(expense: ExpenseIn):
    return expenses.add_expense(
        expense.trip_id,
        expense.payer_id,
        expense.name,
        expense.amount,
        expense.date
    )

@app.get("/get_expenses/{trip_id}")
def get_expenses(trip_id: int):
    return {"expenses": expenses.get_expenses(trip_id)}

@app.put("/update_expense/{expense_id}")
def update_expense(expense_id: int, expense: ExpenseUpdate):
    return expenses.update_expense(
        expense_id,
        expense.payer_id,
        expense.name,
        expense.amount,
        expense.date
    )

@app.delete("/delete_expense/{expense_id}")
def delete_expense(expense_id: int):
    return expenses.delete_expense(expense_id)

# =========================================================
# 💸 ADVANCES
# =========================================================

@app.post("/add_advance")
def add_advance(advance: advances.AdvanceModel):
    return advances.add_advance(
        advance.trip_id,
        advance.payer_family_id,
        advance.receiver_family_id,
        advance.amount,
        advance.date
    )

@app.get("/advances/{trip_id}")
def get_advances(trip_id: int):
    return advances.get_advances(trip_id)

# =========================================================
# 🧾 SETTLEMENT / REPORT
# =========================================================

@app.get("/settlement/{trip_id}")
def get_settlement(trip_id: int):
    return settlement.get_settlement(trip_id)

@app.get("/trip_summary/{trip_id}")
def trip_summary(trip_id: int):
    return settlement.get_trip_summary(trip_id)
@app.get("/version")
def version():
    return {"version": "v1.1 - auto deploy test"}