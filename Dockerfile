FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev

COPY shared /app/shared
COPY facade /app/facade
COPY counter_service /app/counter_service
COPY logging_service /app/logging_service

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app