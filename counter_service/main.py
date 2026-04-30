import os
from fastapi import FastAPI, Request, Depends
from contextlib import asynccontextmanager
from psycopg2.pool import ThreadedConnectionPool


DB_URL = os.getenv("DB_URL", "postgresql://user:password@postgres_db:5432/counter_db")


# https://fastapi.tiangolo.com/advanced/events/
@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = ThreadedConnectionPool(
        minconn=1,
        maxconn=20,
        dsn=DB_URL,
    )
    app.state.db_pool = pool

    conn = pool.getconn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS balances (
                        user_id TEXT PRIMARY KEY,
                        balance NUMERIC(15, 2) NOT NULL DEFAULT 0.0
                    )
                """)
    finally:
        pool.putconn(conn)
    print("[COUNTER] Successfully connected to PostgreSQL!")
    yield

    pool.closeall()


app = FastAPI(lifespan=lifespan)


def get_connection(request: Request):
    pool = request.app.state.db_pool
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


@app.get("/health")
def health_check(conn=Depends(get_connection)):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": str(e)}


@app.post("/transaction")
def count_user_balance(
    request: Request, transaction: dict, conn=Depends(get_connection)
):
    user_id = transaction["user_id"]
    amount = transaction["amount"]

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO balances (user_id, balance)
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO UPDATE
                SET balance = balances.balance + EXCLUDED.balance
                RETURNING balance
                """,
                (user_id, amount),
            )

            result = cur.fetchone()
            new_balance = result[0]

    print(
        f"[COUNTER] User '{user_id}' applied {amount}. New balance: {new_balance}",
        flush=True,
    )

    return {"balance": new_balance}


@app.get("/balance/{user_id}")
def get_user_balance(request: Request, user_id: str, conn=Depends(get_connection)):
    with conn.cursor() as cur:
        cur.execute("SELECT balance FROM balances WHERE user_id = %s", (user_id,))
        result = cur.fetchone()
        balance = result[0] if result else 0.0
        return {"balance": balance}


@app.get("/balance")
def get_all_user_balance(request: Request, conn=Depends(get_connection)):
    with conn.cursor() as cur:
        cur.execute("SELECT user_id, balance FROM balances")
        records = cur.fetchall()
        balances = {record[0]: record[1] for record in records}
        return {"balances": balances}
