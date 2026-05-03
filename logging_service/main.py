import httpx
import os
from hazelcast.client import HazelcastClient
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager

SERVICE_ID = os.getenv("SERVICE_ID", "logging1")
SERVICE_HOST = os.getenv("SERVICE_HOST", "127.0.0.1")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8000"))
CONSUL_URL = os.getenv("CONSUL_URL", "http://consul:8500")


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient() as http_client:
        try:
            kv_resp = await http_client.get(
                f"{CONSUL_URL}/v1/kv/hazelcast/config?raw=true"
            )
            hz_config = kv_resp.json()
            print(f"[LOGGING] Fetched Hazelcast Config: {hz_config}", flush=True)
        except Exception as e:
            print(f"[LOGGING] Failed to fetch Hazelcast config: {e}", flush=True)
            hz_config = {"cluster_name": "dev-cluster", "members": ["hazelcast1:5701"]}

        client = HazelcastClient(
            cluster_name=hz_config["cluster_name"],
            cluster_members=hz_config["members"],
        )
        app.state.hz_client = client
        app.state.distributed_map = client.get_map("messages_map").blocking()
        print("[LOGGING] Connected to Hazelcast cluster!", flush=True)

        registration_payload = {
            "ID": SERVICE_ID,
            "Name": "logging-service",
            "Address": SERVICE_HOST,
            "Port": SERVICE_PORT,
            "Check": {
                "HTTP": f"http://{SERVICE_HOST}:{SERVICE_PORT}/health",
                "Interval": "10s",
                "Timeout": "5s",
                "DeregisterCriticalServiceAfter": "1m",
            },
        }
        try:
            await http_client.put(
                f"{CONSUL_URL}/v1/agent/service/register", json=registration_payload
            )
            print(f"[LOGGING] Registered {SERVICE_ID} with Consul", flush=True)
        except Exception as e:
            print(f"[LOGGING] Consul registration failed: {e}", flush=True)

    yield

    client.shutdown()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def health_check(request: Request):
    try:
        client = request.app.state.hz_client
        if client.lifecycle_service.is_running():
            return {"status": "ok", "hazelcast": "connected"}
        else:
            return {"status": "error", "hazelcast": "disconnected"}
    except Exception as e:
        return {"status": "error", "hazelcast": str(e)}


@app.post("/transaction")
def log_user(request: Request, transaction: dict):
    distributed_map = request.app.state.distributed_map

    transaction_id = transaction["transaction_id"]
    user_id = transaction["user_id"]

    distributed_map.put(transaction_id, transaction)

    print(
        f"[LOGGING] Stored transaction {transaction_id} for user '{user_id}'",
        flush=True,
    )

    return {"status": "success!"}


@app.get("/transaction/{user_id}")
def get_user_transactions(request: Request, user_id: str):
    distributed_map = request.app.state.distributed_map

    all_logs = distributed_map.values()
    transactions = [elm for elm in all_logs if elm.get("user_id") == user_id]

    return {"transactions": transactions}
