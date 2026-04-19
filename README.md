# SA - HW 4: Microservices with Messaging Queue

## Implementation
- FastAPI + Hazelcast + PostgreSQL + Kafka + Config Server

## Architecture Updates
- **Service Discovery** Added a `config_server` where logging and counter services dynamically register their IPs on startup. While facade service queries this registry to locate their URLs.
- **Message Queue** Replaced synchronouns HTTP calls to the counter service with an asynchronous producer/consumer using **Apache Kafka**


## Endpoints
- `POST /process` - Process the transaction (requires `user_id` and `amount`).
- `GET /user/{user_id}` - Retrieve balance and transaction history of a specific user
- `GET /accounts` - Retrieve the balances of all users
- `GET /metrics` - Get the latency for the logging and counter services
- `POST /metrics/reset` - Resets the metrics, setting them to 0
- `GET /health` - Checks the overall system health.

## Build & Run
This command will deploy the config server, kafka, facade (port 8080), counter, and 3 instances of logging services. Also 3 nodes of hazelcast, 1 management service (port 8081) and 1 postgres database.
```{shell}
docker compose up -d --build
```

## Protocol
Here is the [Report](sa_hw4_bilyi.pdf)

## Testing
You can use, for example, `Thunderbolt client` or `/docs` path.

To run the script, make sure to have the necessary dependencies (via uv or pip).

Example usage:
```
uv run --with httpx perf_test.py
```