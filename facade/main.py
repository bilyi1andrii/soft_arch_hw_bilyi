import httpx
import time
import asyncio
from fastapi import FastAPI
from shared.classes import Transaction

app = FastAPI()

LOGGING_SERVICE_URL = "http://logging_service:8000"
COUNTER_SERVICE_URL = "http://counter_service:8000"

METRICS = {"logging_time": 0.0, "counter_time": 0.0}

http_client = httpx.AsyncClient()


async def do_logging(payload: dict):
    start_time = time.perf_counter()
    await http_client.post(f"{LOGGING_SERVICE_URL}/transaction", json=payload)
    METRICS["logging_time"] += time.perf_counter() - start_time


async def do_counting(payload: dict):
    start_time = time.perf_counter()
    resp = await http_client.post(f"{COUNTER_SERVICE_URL}/transaction", json=payload)
    METRICS["counter_time"] += time.perf_counter() - start_time
    return resp.json().get("balance", 0)


@app.post("/process")
async def process_request(transaction: Transaction):
    timestamp_id = str(time.time_ns())

    # Convert to dict
    payload = transaction.model_dump()

    payload["transaction_id"] = timestamp_id

    results = await asyncio.gather(do_logging(payload), do_counting(payload))

    balance = results[1]

    return {"transaction_id": timestamp_id, "balance": balance}


@app.get("/health")
async def get_system_status():
    return {"status": "ok"}


@app.get("/user/{user_id}")
async def get_user_data(user_id: str):

    log_resp = await http_client.get(f"{LOGGING_SERVICE_URL}/transaction/{user_id}")
    transactions = log_resp.json().get("transactions", [])

    count_resp = await http_client.get(f"{COUNTER_SERVICE_URL}/balance/{user_id}")
    balance = count_resp.json().get("balance", 0)

    return {"balance": balance, "transactions": transactions}


@app.get("/accounts")
async def get_all_account_balances():
    count_resp = await http_client.get(f"{COUNTER_SERVICE_URL}/balance")
    balances = count_resp.json().get("balances", {})
    return balances


@app.get("/metrics")
async def get_metrics():
    return METRICS


@app.post("/metrics/reset")
async def reset_metrics():
    METRICS["counter_time"] = 0.0
    METRICS["logging_time"] = 0.0
    return {"message": "Metrics reset successfully!"}
