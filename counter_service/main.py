import asyncpg
import os
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager


DB_URL = os.getenv("DB_URL", "postgresql://user:password@postgres_db:5432/counter_db")


# https://fastapi.tiangolo.com/advanced/events/
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DB_URL)
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS balances (
                user_id TEXT PRIMARY KEY,
                balance NUMERIC(15, 2) NOT NULL DEFAULT 0.0
            )
        """)
    print("[COUNTER] Successfully connected to PostgreSQL!")

    yield

    await app.state.pool.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health_check(request: Request):
    try:
        db_pool = request.app.state.pool
        async with db_pool.acquire() as conn:
            await conn.execute("SELECT 1")
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": str(e)}


@app.post("/transaction")
async def count_user_balance(request: Request, transaction: dict):
    db_pool = request.app.state.pool

    user_id = transaction["user_id"]
    amount = transaction["amount"]

    async with db_pool.acquire() as conn:
        new_balance = await conn.fetchval(
            """
            INSERT INTO balances (user_id, balance)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE
            SET balance = balances.balance + $2
            RETURNING balance
        """,
            user_id,
            amount,
        )

    print(
        f"[COUNTER] User '{user_id}' applied {amount}. New balance: {new_balance}",
        flush=True,
    )

    return {"balance": new_balance}


@app.get("/balance/{user_id}")
async def get_user_balance(request: Request, user_id: str):
    db_pool = request.app.state.pool
    async with db_pool.acquire() as conn:
        balance = await conn.fetchval(
            "SELECT balance FROM balances WHERE user_id = $1", user_id
        )
    return {"balance": balance or 0}


@app.get("/balance")
async def get_all_user_balance(request: Request):
    db_pool = request.app.state.pool
    async with db_pool.acquire() as conn:
        records = await conn.fetch("SELECT user_id, balance FROM balances")
    balances = {record["user_id"]: record["balance"] for record in records}
    return {"balances": balances}
