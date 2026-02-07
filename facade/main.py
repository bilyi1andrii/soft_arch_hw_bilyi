import httpx
import time
from fastapi import FastAPI
from shared.classes import Transaction

app = FastAPI()

LOGGING_SERVICE_URL = "http://logging_service:8000"
COUNTER_SERVICE_URL = "http://counter_service:8000"

@app.post("/process")
async def process_request(transaction: Transaction):
    timestamp_id = str(time.time_ns())

    # Convert to dict
    payload = transaction.model_dump()

    payload["transaction_id"] = timestamp_id

    async with httpx.AsyncClient() as client:
        await client.post(f"{LOGGING_SERVICE_URL}/log", json=payload)

        resp = await client.post(f"{COUNTER_SERVICE_URL}/count", json=payload)

        balance = resp.json().get("balance", 0)

    return {"transaction_id": timestamp_id, "balance": balance}


@app.get("/")
async def hello():
    return {"msg": "bye"}

@app.get("/user/{user_id}")
async def get_user_data(user_id: str):

    async with httpx.AsyncClient() as client:
        log_resp = await client.get(f"{LOGGING_SERVICE_URL}/transaction/{user_id}")
        transactions = log_resp.json().get("transactions", [])

        count_resp = await client.get(f"{COUNTER_SERVICE_URL}/balance/{user_id}")
        balance = count_resp.json().get("balance", 0)

    return {"balance": balance, "transactions": transactions}

@app.get("/accounts")
async def get_all_account_balances():

    async with httpx.AsyncClient() as client:
        count_resp = await client.get(f"{COUNTER_SERVICE_URL}/balance")
        balances = count_resp.json().get("balances", {})

    return balances