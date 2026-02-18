from fastapi import FastAPI

app = FastAPI()
user_logs = {}


@app.post("/transaction")
def log_user(transaction: dict):

    transaction_id = transaction["transaction_id"]
    user_id = transaction["user_id"]

    user_logs[transaction_id] = transaction

    print(f"[LOGGING] Stored transaction {transaction_id} for user '{user_id}'", flush=True)

    return {"status": "success!"}


@app.get("/transaction/{user_id}")
async def get_user_transactions(user_id: str):
    transactions = [elm for elm in user_logs.values() if elm.get("user_id") == user_id]

    return {"transactions": transactions}
