import httpx
from fastapi import FastAPI
import uuid
from shared.classes import Request, Response

app = FastAPI()

LOGGING_URL = "http://logging:8000"

@app.post("/process")
async def process_transaction(request: Request):
    return {"status": "received"}


@app.get("/")
async def hello():
    return {"msg": "hello"}