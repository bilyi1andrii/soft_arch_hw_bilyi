from fastapi import FastAPI

app = FastAPI()
user_logs = {}


@app.post("/transaction")
def log_user(transaction: dict):

    user_logs[transaction["transaction_id"]] = transaction

    return {"status": "success!"}


@app.get("/transaction/{user_id}")
async def get_user_transactions(user_id: str):
    transactions = [elm for elm in user_logs.values() if elm.get("user_id") == user_id]

    return {"transactions": transactions}
