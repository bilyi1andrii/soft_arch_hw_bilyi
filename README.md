# SA - HW 3: Microservices with Hazelcast

## Implementation
- FastAPI + Hazelcast + PostgreSQL

## Endpoints
- `POST /process` - Process the transaction (requires `user_id` and `amount`)
- `GET /user/{user_id}` - Retrieve balance and transaction history of a specific user
- `GET /accounts` - Retrieve the balances of all users
- `GET /metrics` - Get the latency for the logging and counter services
- `POST /metrics/reset` - Resets the metrics, setting them to 0

## Build & Run
This command will deploy facade (port 8080), counter, and 3 instances of logging services. Also 3 nodes of hazelcast, 1 management service (port 8081) and 1 postgres.
```{shell}
docker compose up -d --build
```

## Protocol
Saved as sa_hw3_bilyi.pdf

## Testing
You can use, for example, `Thunderbolt client` or `/docs` path.

To run the script, make sure to have the necessary dependencies (via uv or pip).

Example usage:
```
uv run --with httpx perf_test.py
```