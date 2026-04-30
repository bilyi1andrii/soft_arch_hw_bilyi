import os
import json
import asyncio
import httpx
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, Request, Depends
from contextlib import asynccontextmanager
from psycopg2.pool import ThreadedConnectionPool


DB_URL = os.getenv("DB_URL", "postgresql://user:password@postgres_db:5432/counter_db")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
MY_URL = os.getenv("MY_URL", "http://counter_service:8000")
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config_server:8000")


async def consume_transactions(app: FastAPI):
    consumer = AIOKafkaConsumer(
        "transactions",
        bootstrap_servers=KAFKA_BROKERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="counter-group",
    )
    await consumer.start()
    print(
        "[COUNTER] Kafka Consumer started listening to 'transactions' topic", flush=True
    )

    try:
        async for msg in consumer:
            transaction = msg.value

            if transaction is None:
                continue

            user_id = transaction["user_id"]
            amount = transaction["amount"]

            # Process DB insert manually from pool inside async task
            pool = app.state.db_pool
            conn = pool.getconn()
            try:
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
                            f"[COUNTER] Processed Kafka msg. User '{user_id}' applied {amount}. New balance: {new_balance}",
                            flush=True,
                        )
            except Exception as e:
                print(f"[COUNTER] DB Error processing message: {e}", flush=True)
            finally:
                pool.putconn(conn)
    finally:
        await consumer.stop()


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

    async with httpx.AsyncClient() as http_client:
        try:
            await http_client.post(
                f"{CONFIG_SERVER_URL}/register",
                json={"name": "counter-service", "url": MY_URL},
            )
            print("[COUNTER] Registered with config server")
        except Exception as e:
            print(f"[COUNTER] Registration failed: {e}")

    consumer_task = asyncio.create_task(consume_transactions(app))
    yield

    consumer_task.cancel()
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
