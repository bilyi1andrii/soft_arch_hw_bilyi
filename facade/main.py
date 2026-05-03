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


CONSUL_URL = os.getenv("CONSUL_URL", "http://consul:8500")
SERVICE_ID = os.getenv("SERVICE_ID", "facade1")
SERVICE_HOST = os.getenv("SERVICE_HOST", "127.0.0.1")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8000"))

METRICS = {"logging_time": 0.0, "counter_time": 0.0}


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http_client = httpx.AsyncClient()

    try:
        kv_resp = await app.state.http_client.get(
            f"{CONSUL_URL}/v1/kv/kafka/config?raw=true"
        )
        kafka_config = kv_resp.json()
        kafka_brokers = kafka_config.get("brokers", "kafka:9092")

        kafka_topic = kafka_config.get("topic", "transactions")
        app.state.kafka_topic = kafka_topic

        print(f"[FACADE] Fetched Kafka Config: {kafka_config}", flush=True)
    except Exception as e:
        print(f"[FACADE] Failed to fetch Kafka config: {e}", flush=True)
        kafka_brokers = "kafka:9092"
        app.state.kafka_topic = "transactions"

    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_brokers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    app.state.kafka_producer = producer
    print("[FACADE] Connected to Kafka", flush=True)

    registration_payload = {
        "ID": SERVICE_ID,
        "Name": "facade-service",
        "Address": SERVICE_HOST,
        "Port": SERVICE_PORT,
        "Check": {
            "HTTP": f"http://{SERVICE_HOST}:{SERVICE_PORT}/health",
            "Interval": "10s",
            "Timeout": "5s",
            "DeregisterCriticalServiceAfter": "1m"
        }
    }
    try:
        await app.state.http_client.put(f"{CONSUL_URL}/v1/agent/service/register", json=registration_payload)
        print(f"[FACADE] Registered {SERVICE_ID} with Consul", flush=True)
    except Exception as e:
        print(f"[FACADE] Consul registration failed: {e}", flush=True)

    yield

    await producer.stop()
    await app.state.http_client.aclose()

app = FastAPI(lifespan=lifespan)


async def get_service_urls(http_client: httpx.AsyncClient, service_name: str) -> list:
    try:
        resp = await http_client.get(
            f"{CONSUL_URL}/v1/health/service/{service_name}?passing=true"
        )
        if resp.status_code == 200:
            instances = resp.json()
            urls = []
            for instance in instances:
                address = instance["Service"]["Address"]
                port = instance["Service"]["Port"]
                urls.append(f"http://{address}:{port}")
            return urls
    except Exception as e:
        print(f"[FACADE] Error fetching {service_name} from Consul: {e}")
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


async def do_counting(producer: AIOKafkaProducer, payload: dict, topic: str):
    start_time = time.perf_counter()
    await producer.send_and_wait(topic, payload)
    METRICS["counter_time"] += time.perf_counter() - start_time
    return "Queued via Kafka"


@app.post("/process")
async def process_request(request: Request, transaction: Transaction):
    http_client = request.app.state.http_client
    kafka_producer = request.app.state.kafka_producer
    kafka_topic = request.app.state.kafka_topic
    timestamp_id = str(time.time_ns())

    # Convert to dict
    payload = transaction.model_dump()
    payload["transaction_id"] = timestamp_id

    print(
        f"[FACADE] Received request from user '{transaction.user_id}' for amount {transaction.amount}",
        flush=True,
    )

    await asyncio.gather(
        do_logging(http_client, payload), do_counting(kafka_producer, payload, kafka_topic)
    )

    return {"transaction_id": timestamp_id, "status": "Message queued successfully"}


@app.get("/health")
async def get_system_status(request: Request):
    http_client = request.app.state.http_client
    producer = request.app.state.kafka_producer

    consul_status = "down"
    try:
        resp = await http_client.get(f"{CONSUL_URL}/v1/status/leader", timeout=2.0)
        if resp.status_code == 200:
            consul_status = "ok"
    except httpx.RequestError:
        pass

    if consul_status == "down":
        return {
            "status": "degraded",
            "consul": "down",
            "counter_services": {},
            "logging_services": {},
        }

    counter_instances = await get_service_urls(http_client, "counter-service")
    logging_instances = await get_service_urls(http_client, "logging-service")

    async def fetch_health(url: str):
        try:
            resp = await http_client.get(f"{url}/health", timeout=2.0)
            return url, resp.json() if resp.status_code == 200 else "error"
        except httpx.RequestError:
            return url, "down"

    counter_tasks = [fetch_health(url) for url in counter_instances]
    logging_tasks = [fetch_health(url) for url in logging_instances]

    counter_results = await asyncio.gather(*counter_tasks)
    logging_results = await asyncio.gather(*logging_tasks)

    counter_statuses = {url: status for url, status in counter_results}
    logging_statuses = {url: status for url, status in logging_results}

    is_counter_ok = len(counter_instances) > 0 and any(
        isinstance(status, dict) and status.get("status") == "ok"
        for status in counter_statuses.values()
    )

    is_logging_ok = len(logging_instances) > 0 and any(
        isinstance(status, dict) and status.get("status") == "ok"
        for status in logging_statuses.values()
    )

    kafka_status = "ok" if producer else "down"

    is_system_ready = (
        is_counter_ok
        and is_logging_ok
        and consul_status == "ok"
        and kafka_status == "ok"
    )
    overall_status = "ok" if is_system_ready else "degraded"

    return {
        "status": overall_status,
        "consul": consul_status,
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
