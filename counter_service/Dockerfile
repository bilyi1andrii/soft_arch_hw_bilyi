FROM python:3.12-slim-bookworm
COPY --from=docker.io/astral/uv:latest /uv /uvx /bin/

WORKDIR /app


COPY ./pyproject.toml ./uv.lock ./
RUN uv sync


COPY ./shared /app/shared
COPY ./counter_service /app/counter_service


CMD ["uv", "run", "fastapi", "run", "counter_service/main.py", "--host", "0.0.0.0", "--port", "8000"]