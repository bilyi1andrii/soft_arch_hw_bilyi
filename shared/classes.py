from pydantic import BaseModel

class Request(BaseModel):
    user_id: str
    amount: int

class Response(BaseModel):
    transaction_id: str
    msg: int