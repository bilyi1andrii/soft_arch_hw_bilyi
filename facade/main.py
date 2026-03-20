import httpx
import time
import asyncio
import random
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from shared.classes import Transaction

LOGGING_INSTANCES = [
    "http://logging1:8000",
    "http://logging2:8000",
    "http://logging3:8000",
]
COUNTER_SERVICE_URL = "http://counter_service:8000"
METRICS = {"logging_time": 0.0, "counter_time": 0.0}


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http_client = httpx.AsyncClient()
    yield
    await app.state.http_client.aclose()


app = FastAPI(lifespan=lifespan)


async def do_logging(http_client: httpx.AsyncClient, payload: dict):
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


async def do_counting(http_client: httpx.AsyncClient, payload: dict):
    start_time = time.perf_counter()
    resp = await http_client.post(f"{COUNTER_SERVICE_URL}/transaction", json=payload)
    METRICS["counter_time"] += time.perf_counter() - start_time
    return resp.json().get("balance", 0.0)


@app.post("/process")
async def process_request(request: Request, transaction: Transaction):
    http_client = request.app.state.http_client
    timestamp_id = str(time.time_ns())

    # Convert to dict
    payload = transaction.model_dump()
    payload["transaction_id"] = timestamp_id

    print(
        f"[FACADE] Received request from user '{transaction.user_id}' for amount {transaction.amount}",
        flush=True,
    )

    results = await asyncio.gather(
        do_logging(http_client, payload), do_counting(http_client, payload)
    )

    balance = results[1]

    print(
        f"[FACADE] Completed transaction {timestamp_id}. Final balance: {balance}",
        flush=True,
    )

    return {"transaction_id": timestamp_id, "balance": balance}


@app.get("/health")
async def get_system_status(request: Request):
    http_client = request.app.state.http_client

    counter_status = "down"
    try:
        resp = await http_client.get(f"{COUNTER_SERVICE_URL}/health", timeout=2.0)
        if resp.status_code == 200:
            counter_status = resp.json()
    except httpx.RequestError:
        pass

    logging_statuses = {}
    for url in LOGGING_INSTANCES:
        try:
            resp = await http_client.get(f"{url}/health", timeout=2.0)
            if resp.status_code == 200:
                logging_statuses[url] = resp.json()
            else:
                logging_statuses[url] = "error"
        except httpx.RequestError:
            logging_statuses[url] = "down"

    is_counter_ok = counter_status != "down" and counter_status.get("status") == "ok"
    is_logging_ok = any(
        status != "down" and status.get("status") == "ok"
        for status in logging_statuses.values()
    )

    overall_status = "ok" if (is_counter_ok and is_logging_ok) else "degraded"

    return {
        "status": overall_status,
        "counter_service": counter_status,
        "logging_services": logging_statuses,
    }


@app.get("/user/{user_id}")
async def get_user_data(request: Request, user_id: str):
    http_client = request.app.state.http_client
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
    balance = count_resp.json().get("balance", 0.0)

    return {"balance": balance, "transactions": transactions}


@app.get("/accounts")
async def get_all_account_balances(request: Request):
    http_client = request.app.state.http_client
    count_resp = await http_client.get(f"{COUNTER_SERVICE_URL}/balance")
    return count_resp.json().get("balances", {})


@app.get("/metrics")
async def get_metrics():
    return METRICS


@app.post("/metrics/reset")
async def reset_metrics():
    METRICS["counter_time"] = 0.0
    METRICS["logging_time"] = 0.0
    return {"message": "Metrics reset successfully!"}
