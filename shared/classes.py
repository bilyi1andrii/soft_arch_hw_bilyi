from pydantic import BaseModel

class Transaction(BaseModel):
    user_id: str
    amount: int
