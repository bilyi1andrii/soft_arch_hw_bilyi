from hazelcast.client import HazelcastClient
from fastapi import FastAPI


app = FastAPI()

client = HazelcastClient(
    cluster_name="dev-cluster",
    cluster_members=[
        "hazelcast1:5701",
        "hazelcast2:5701",
        "hazelcast3:5701"
    ]
)

distributed_map = client.get_map("messages_map").blocking()


@app.post("/transaction")
def log_user(transaction: dict):
    transaction_id = transaction["transaction_id"]
    user_id = transaction["user_id"]

    distributed_map.put(transaction_id, transaction)

    print(f"[LOGGING] Stored transaction {transaction_id} for user '{user_id}'", flush=True)

    return {"status": "success!"}


@app.get("/transaction/{user_id}")
async def get_user_transactions(user_id: str):
    all_logs = distributed_map.values()
    transactions = [elm for elm in all_logs if elm.get("user_id") == user_id]

    return {"transactions": transactions}
