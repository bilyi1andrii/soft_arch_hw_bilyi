from fastapi import FastAPI

app = FastAPI()

users_balance = {}


@app.post("/count")
async def count_user_balance(transaction: dict):

    user_id = transaction["user_id"]
    amount = transaction["amount"]

    users_balance[user_id] = users_balance.get(user_id, 0) + amount

    return {"balance": users_balance[user_id]}


@app.get("/balance/{user_id}")
async def get_user_balance(user_id: str):
    return {"balance": users_balance.get(user_id, 0)}


@app.get("/balance")
async def get_all_user_balance():
    return {"balances": users_balance}
