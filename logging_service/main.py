import httpx
import os
from hazelcast.client import HazelcastClient
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager

MY_URL = os.getenv("MY_URL", "http://logging:8000")
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config_server:8000")


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = HazelcastClient(
        cluster_name="dev-cluster",
        cluster_members=["hazelcast1:5701", "hazelcast2:5701", "hazelcast3:5701"],
    )
    app.state.hz_client = client
    app.state.distributed_map = client.get_map("messages_map").blocking()

    print("[LOGGING] Connected to Hazelcast cluster!", flush=True)

    async with httpx.AsyncClient() as http_client:
        try:
            await http_client.post(
                f"{CONFIG_SERVER_URL}/register",
                json={"name": "logging-service", "url": MY_URL},
            )
            print(
                f"[LOGGING] Successfully registered {MY_URL} to config-server",
                flush=True,
            )
        except Exception as e:
            print(f"[LOGGING] Failed to register with config server: {e}", flush=True)

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
