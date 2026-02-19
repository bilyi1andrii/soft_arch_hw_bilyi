# SA - HW 1: Basic Microservices Architecture

## Implementation
- Python + FastAPI

## Endpoints
- `POST /process` - Process the transaction (requires `user_id` and `amount`)
- `GET /user/{user_id}` - Retrieve balance and transaction history of a specific user
- `GET /accounts` - Retrieve the balances of all users
- `GET /metrics` - Get the latency for the logging and counter services
- `POST /metrics/reset` - Resets the metrics, setting them to 0

## Build & Run
```{shell}
docker compose up -d --build
```

## Testing
You can use, for example, `Thunderbolt client` or `/docs` path.

To run the script, make sure to have the necessary dependencies (via uv or pip).

Example usage:
```
uv run --with httpx perf_test.py
```
