from hazelcast.client import HazelcastClient
import asyncio


async def main():
    client = HazelcastClient(
        cluster_name="dev-cluster",
        cluster_members=["hazelcast1:5701", "hazelcast2:5701", "hazelcast3:5701"],
    )

    print("Connected to Hazelcast cluster.")
    distributed_map = client.get_map("step3_demo_map")

    print("Writing 1000 values...")
    for i in range(1000):
        distributed_map.put(str(i), f"value_{i}")

    print("Finished writing data. Check the Management Center!")

    await asyncio.sleep(2)
    client.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
