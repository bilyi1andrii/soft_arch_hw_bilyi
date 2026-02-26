# SA - HW 2: Intro to Hazelcast: Distributed Map

## Implementation
- Docker + Python Client

## Build & Run
This command will deploy 3 nodes of hazelcast and management service. Additionally, it runs 3 services from the previous task.
```{shell}
docker compose up -d --build
```

## Protocol
Saved as sa_hw2_protocol_bilyi.pdf

## Testing
After deploying the containers. You can run the following scripts for the corresponding step.

**Step 3**
```{shell}
docker-compose exec facade uv run python step3_demo.py
```

Possible output:
```
Connected to Hazelcast cluster.
Writing 1000 values...
Finished writing data. Check the Management Center!
```


**Step 4-7**
```{shell}
docker-compose exec facade uv run python step456_locks.py
```

Possible output:
```
Starting NO LOCKS test
Final value: 15764
Time: 5.13s

Starting PESSIMISTIC LOCKING test
Final value: 30000
Time: 18.43s

Starting OPTIMISTIC LOCKING test
Final value: 30000
Time: 11.71s
```

**Step 8**
```{shell}
docker-compose exec facade uv run python step8_queue.py
```

Possible output:
```
[Producer] Starting...
[Reader 1] Ready for reading
[Reader 2] Ready for reading
  -> [Reader 1] Read: 1
[Producer] Wrote: 1
  -> [Reader 2] Read: 2
[Producer] Wrote: 2
  -> [Reader 1] Read: 3
[Producer] Wrote: 3
  -> [Reader 2] Read: 4
...
```