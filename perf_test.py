import asyncio
import time
import httpx


FACADE_URL = "http://localhost:8080"
NUM_CLIENTS = 10
REQUESTS_PER_CLIENT = 10000


async def client_worker(client: httpx.AsyncClient, user_id: str, amount: int):
    for _ in range(REQUESTS_PER_CLIENT):
        payload = {"user_id": user_id, "amount": amount}
        await client.post(f"{FACADE_URL}/process", json=payload)


async def run_experiment(same_account: bool):
    async with httpx.AsyncClient() as client:
        await client.post(f"{FACADE_URL}/metrics/reset")

    async with httpx.AsyncClient() as client:
        tasks = []
        for i in range(NUM_CLIENTS):
            user_id = "shared" if same_account else f"user_{i}"
            tasks.append(client_worker(client, user_id, 1))
        start_time = time.perf_counter()
        await asyncio.gather(*tasks)

        end_time = time.perf_counter()

    total_requests = NUM_CLIENTS * REQUESTS_PER_CLIENT
    total_time = end_time - start_time
    rps = total_requests / total_time

    print("\n--- Performance Results ---")
    print(f"Total Requests: {total_requests}")
    print(f"Total Time: {total_time:.2f} seconds")
    print(f"Requests Per Second (RPS): {rps:.2f}")

    async with httpx.AsyncClient() as client:
        metrics_resp = await client.get(f"{FACADE_URL}/metrics")
        metrics = metrics_resp.json()

        print("\n--- Internal Time Contributions ---")
        print(f"Logging Service Total Delay: {metrics.get('logging_time', 0):.2f}s")
        print(f"Counter Service Total Delay: {metrics.get('counter_time', 0):.2f}s")

        print("\n--- Balance Verification ---")
        accounts_resp = await client.get(f"{FACADE_URL}/accounts")
        balances = accounts_resp.json()

        if same_account:
            actual_balance = balances.get("shared", 0)
            print(f"Shared Account Balance: {actual_balance} (Expected: 100000)")
            if actual_balance != 100000:
                print("WARNING: Race condition detected! Data was lost.")
        else:
            for i in range(NUM_CLIENTS):
                uid = f"user_{i}"
                actual_balance = balances.get(uid, 0)
                print(f"{uid} Balance: {actual_balance} (Expected: 10000)")
                if actual_balance != 10000:
                    print(f"WARNING: Race condition detected for {uid}!")


async def main():
    print(f"\n{'=' * 40}")
    print(f"Starting {'Scenario 1 (10 Clients, 10 Unique Accounts)'}")
    print(f"{'=' * 40}")
    await run_experiment(same_account=False)

    print(f"\n{'=' * 40}")
    print(f"Starting {'Scenario 2 (10 Clients, 1 Shared Account)'}")
    print(f"{'=' * 40}")
    await run_experiment(same_account=True)


if __name__ == "__main__":
    asyncio.run(main())
