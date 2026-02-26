import time
import threading
from hazelcast.client import HazelcastClient


client = HazelcastClient(
    cluster_name="dev-cluster",
    cluster_members=["hazelcast1:5701", "hazelcast2:5701", "hazelcast3:5701"]
)
hz_map = client.get_map("lock_map")

ITERATIONS = 10000

def run_no_locks(client_id):
    for k in range(ITERATIONS):
        value = hz_map.get("key").result() or 0
        value += 1
        hz_map.put("key", value).result()

def run_pessimistic(client_id):
    for k in range(ITERATIONS):
        hz_map.lock("key").result()
        try:
            value = hz_map.get("key").result() or 0
            value += 1
            hz_map.put("key", value).result()
        finally:
            hz_map.unlock("key").result()

def run_optimistic(client_id):
    for k in range(ITERATIONS):
        while True:
            old_value = hz_map.get("key").result() or 0
            new_value = old_value + 1
            if hz_map.replace_if_same("key", old_value, new_value).result():
                break


def execute_test(mode_name, target_function):
    hz_map.put("key", 0).result()

    threads = []
    for i in range(3):
        t = threading.Thread(target=target_function, args=(i,))
        threads.append(t)

    print(f"\nStarting {mode_name} test")
    start_time = time.perf_counter()

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    end_time = time.perf_counter()
    final_value = hz_map.get("key").result()

    print(f"Final value: {final_value}")
    print(f"Time: {end_time - start_time:.2f}s")

if __name__ == "__main__":
    try:
        execute_test("NO LOCKS", run_no_locks)
        execute_test("PESSIMISTIC LOCKING", run_pessimistic)
        execute_test("OPTIMISTIC LOCKING", run_optimistic)
    finally:
        client.shutdown()