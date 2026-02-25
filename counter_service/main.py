from hazelcast.client import HazelcastClient
from fastapi import FastAPI
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    client = HazelcastClient(
        cluster_name="dev-cluster",
        cluster_members=[
            "hazelcast1:5701",
            "hazelcast2:5701",
            "hazelcast3:5701"
        ]
    )

    app.state.hz_client = client
    app.state.users_balance = client.get_map("users_balance")
    yield
    client.shutdown()

app = FastAPI(lifespan=lifespan)

@app.post("/transaction")
async def count_user_balance(transaction: dict):
    user_id = transaction["user_id"]
    amount = transaction["amount"]

    hz_map = app.state.users_balance


    current_balance = await hz_map.get(user_id) or 0
    new_balance = current_balance + amount


    await hz_map.put(user_id, new_balance)

    print(f"[COUNTER] User '{user_id}' applied {amount}. New balance: {new_balance}", flush=True)

    return {"balance": new_balance}

@app.get("/balance/{user_id}")
async def get_user_balance(user_id: str):
    hz_map = app.state.users_balance
    balance = await hz_map.get(user_id) or 0
    return {"balance": balance}

@app.get("/balance")
async def get_all_user_balance():
    hz_map = app.state.users_balance

    balances = await hz_map.entry_set()

    balances_dict = {k: v for k, v in balances}
    return {"balances": balances_dict}