# SA - HW 5: Microservices with Service Discovery & Config Server (Consul)

## Implementation
- FastAPI + Hazelcast + PostgreSQL + Kafka + Consul

## Architecture Updates
- **Service Discovery & Registry:** Integrated **Consul**. All microservices (Facade, Logging, Counter) dynamically register themselves upon startup. The Facade service queries Consul's health endpoints to discover healthy instances of the Logging and Counter services.
- **Centralized Configuration:** Consul acts as a Key/Value Config Server. The Logging service dynamically fetches Hazelcast cluster configurations, while the Facade and Counter services fetch Kafka broker and topic configurations from Consul on startup.


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
Here is the [Report](sa_hw5_bilyi.pdf)

## Testing
You can use, for example, `Thunderbolt client` or `/docs` path.

To run the script, make sure to have the necessary dependencies (via uv or pip).

Example usage:
```
uv run --with httpx perf_test.py
```