import threading
from hazelcast.client import HazelcastClient

# Change to False for no readers scenario
START_READERS = True


def producer(queue):
    print("[Producer] Starting...")
    for i in range(1, 101):
        queue.put(i).result()
        print(f"[Producer] Wrote: {i}")


def consumer(queue, consumer_id):
    print(f"[Reader {consumer_id}] Ready for reading")
    try:
        while True:
            item = queue.take().result()
            print(f"  -> [Reader {consumer_id}] Read: {item}")
    except Exception:
        ...


if __name__ == "__main__":
    client = HazelcastClient(
        cluster_name="dev-cluster",
        cluster_members=["hazelcast1:5701", "hazelcast2:5701", "hazelcast3:5701"],
    )

    try:
        hz_queue = client.get_queue("bounded_queue")
        hz_queue.clear().result()

        threads = []

        prod_thread = threading.Thread(target=producer, args=(hz_queue,))
        threads.append(prod_thread)

        if START_READERS:
            for i in range(1, 3):
                cons_thread = threading.Thread(target=consumer, args=(hz_queue, i))

                # Finish when main program ends
                cons_thread.daemon = True
                threads.append(cons_thread)

        for t in threads:
            t.start()

        prod_thread.join()

        print("\nTest Complete!")

    finally:
        client.shutdown()
