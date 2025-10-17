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
    start_date: str
    trip_type: str
    owner_id: int  # new field


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
