from pydantic import BaseModel
from typing import Optional

# ------------------ USERS ------------------
class UserIn(BaseModel):
    name: str
    phone: Optional[str] = None
    email: Optional[str] = None


# ------------------ TRIPS ------------------
class TripIn(BaseModel):
    name: str
    start_date: Optional[str] = None
    trip_type: Optional[str] = None
    owner_name: Optional[str] = None
    owner_id: Optional[int] = None
    mode: Optional[str] = "TRIP"          # new field: TRIP or STAY
    billing_cycle: Optional[str] = "MONTHLY"
    end_date: Optional[str] = None



# ------------------ FAMILIES ------------------
class FamilyIn(BaseModel):
    trip_id: int
    family_name: str
    members_count: int


class FamilyUpdate(BaseModel):
    family_name: str
    members_count: int


# ------------------ EXPENSES ------------------
class ExpenseIn(BaseModel):
    trip_id: int
    payer_id: int
    name: str
    amount: float
    date: str


class ExpenseUpdate(BaseModel):
    payer_id: int
    name: str
    amount: float
    date: str


# ------------------ ADVANCES ------------------
class AdvanceModel(BaseModel):
    trip_id: int
    payer_family_id: int
    receiver_family_id: int
    amount: float
    date: str
