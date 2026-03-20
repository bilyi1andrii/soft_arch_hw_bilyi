import httpx
import time
import asyncio
import random
from fastapi import FastAPI
from shared.classes import Transaction

app = FastAPI()

LOGGING_INSTANCES = [
    "http://logging1:8000",
    "http://logging2:8000",
    "http://logging3:8000",
]

COUNTER_SERVICE_URL = "http://counter_service:8000"

METRICS = {"logging_time": 0.0, "counter_time": 0.0}

http_client = httpx.AsyncClient()


async def do_logging(payload: dict):
    start_time = time.perf_counter()
    instances = list(LOGGING_INSTANCES)
    random.shuffle(instances)

    success = False
    for url in instances:
        try:
            await http_client.post(f"{url}/transaction", json=payload)
            success = True
            break
        except httpx.RequestError:
            print(
                f"[FACADE] Failed to connect to {url}, falling back to next instance...",
                flush=True,
            )
            continue
    if not success:
        print("[FACADE] Critical: All logging instances are down!")

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

    print(
        f"[FACADE] Received request from user '{transaction.user_id}' for amount {transaction.amount}",
        flush=True,
    )

    results = await asyncio.gather(do_logging(payload), do_counting(payload))

    balance = results[1]

    print(
        f"[FACADE] Completed transaction {timestamp_id}. Final balance: {balance}",
        flush=True,
    )

    return {"transaction_id": timestamp_id, "balance": balance}


@app.get("/health")
async def get_system_status():
    return {"status": "ok"}


@app.get("/user/{user_id}")
async def get_user_data(user_id: str):
    instances = list(LOGGING_INSTANCES)
    random.shuffle(instances)

    transactions = []
    for url in instances:
        try:
            log_resp = await http_client.get(f"{url}/transaction/{user_id}")
            transactions = log_resp.json().get("transactions", [])
            break
        except httpx.RequestError:
            continue

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
