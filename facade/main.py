import httpx
import time
import asyncio
import random
import json
import os
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from shared.classes import Transaction


CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config_server:8000")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
METRICS = {"logging_time": 0.0, "counter_time": 0.0}


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http_client = httpx.AsyncClient()

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    app.state.kafka_producer = producer
    print("[FACADE] Connected to Kafka", flush=True)

    yield

    await producer.stop()
    await app.state.http_client.aclose()


app = FastAPI(lifespan=lifespan)


async def get_service_urls(http_client: httpx.AsyncClient, service_name: str) -> list:
    try:
        resp = await http_client.get(f"{CONFIG_SERVER_URL}/services/{service_name}")
        if resp.status_code == 200:
            return resp.json().get("instances", [])
    except Exception as e:
        print(f"[FACADE] Error fetching {service_name} from config server: {e}")
    return []


async def do_logging(http_client: httpx.AsyncClient, payload: dict):
    start_time = time.perf_counter()
    instances = await get_service_urls(http_client, "logging-service")

    if not instances:
        print("[FACADE]: No logging instances found in registry!", flush=True)
        return

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
        print("[FACADE]: All logging instances are down!")

    METRICS["logging_time"] += time.perf_counter() - start_time


async def do_counting(producer: AIOKafkaProducer, payload: dict):
    start_time = time.perf_counter()
    await producer.send_and_wait("transactions", payload)
    METRICS["counter_time"] += time.perf_counter() - start_time
    return "Queued via Kafka"


@app.post("/process")
async def process_request(request: Request, transaction: Transaction):
    http_client = request.app.state.http_client
    kafka_producer = request.app.state.kafka_producer
    timestamp_id = str(time.time_ns())

    # Convert to dict
    payload = transaction.model_dump()
    payload["transaction_id"] = timestamp_id

    print(
        f"[FACADE] Received request from user '{transaction.user_id}' for amount {transaction.amount}",
        flush=True,
    )

    await asyncio.gather(
        do_logging(http_client, payload), do_counting(kafka_producer, payload)
    )

    return {"transaction_id": timestamp_id, "status": "Message queued successfully"}


@app.get("/health")
async def get_system_status(request: Request):
    http_client = request.app.state.http_client
    producer = request.app.state.kafka_producer

    config_status = "down"
    try:
        resp = await http_client.get(f"{CONFIG_SERVER_URL}/health", timeout=2.0)
        if resp.status_code == 200:
            config_status = "ok"
    except httpx.RequestError:
        pass

    if config_status == "down":
        return {
            "status": "degraded",
            "config_server": "down",
            "counter_services": {},
            "logging_services": {},
        }

    counter_instances = await get_service_urls(http_client, "counter-service")
    logging_instances = await get_service_urls(http_client, "logging-service")

    counter_statuses = {}
    for url in counter_instances:
        try:
            resp = await http_client.get(f"{url}/health", timeout=2.0)
            if resp.status_code == 200:
                counter_statuses[url] = resp.json()
            else:
                counter_statuses[url] = "error"
        except httpx.RequestError:
            counter_statuses[url] = "down"

    logging_statuses = {}
    for url in logging_instances:
        try:
            resp = await http_client.get(f"{url}/health", timeout=2.0)
            if resp.status_code == 200:
                logging_statuses[url] = resp.json()
            else:
                logging_statuses[url] = "error"
        except httpx.RequestError:
            logging_statuses[url] = "down"

    is_counter_ok = any(
        status != "down" and status != "error" and status.get("status") == "ok"
        for status in counter_statuses.values()
    )
    is_logging_ok = any(
        status != "down" and status != "error" and status.get("status") == "ok"
        for status in logging_statuses.values()
    )

    kafka_status = "ok" if producer and producer.client else "down"

    is_system_ready = (
        is_counter_ok
        and is_logging_ok
        and config_status == "ok"
        and kafka_status == "ok"
    )
    overall_status = "ok" if is_system_ready else "degraded"

    return {
        "status": overall_status,
        "config_server": config_status,
        "kafka": kafka_status,
        "counter_services": counter_statuses,
        "logging_services": logging_statuses,
    }


@app.get("/user/{user_id}")
async def get_user_data(request: Request, user_id: str):
    http_client = request.app.state.http_client
    logging_instances = await get_service_urls(http_client, "logging-service")
    random.shuffle(logging_instances)

    transactions = []
    for url in logging_instances:
        try:
            log_resp = await http_client.get(f"{url}/transaction/{user_id}")
            transactions = log_resp.json().get("transactions", [])
            break
        except httpx.RequestError:
            continue

    counter_instances = await get_service_urls(http_client, "counter-service")
    balance = 0.0
    if counter_instances:
        try:
            # We assume one counter service instance, or just pick the first
            count_resp = await http_client.get(
                f"{counter_instances[0]}/balance/{user_id}"
            )
            balance = count_resp.json().get("balance", 0.0)
        except httpx.RequestError:
            balance = None
    else:
        balance = None

    return {"balance": balance, "transactions": transactions}


@app.get("/accounts")
async def get_all_account_balances(request: Request):
    http_client = request.app.state.http_client
    counter_instances = await get_service_urls(http_client, "counter-service")

    balances = {}

    if counter_instances:
        try:
            count_resp = await http_client.get(f"{counter_instances[0]}/balance")
            balances = count_resp.json().get("balances", {})
        except httpx.RequestError:
            balances = None
    else:
        balances = None

    return balances


@app.get("/metrics")
async def get_metrics():
    return METRICS


@app.post("/metrics/reset")
async def reset_metrics():
    METRICS["counter_time"] = 0.0
    METRICS["logging_time"] = 0.0
    return {"message": "Metrics reset successfully!"}
