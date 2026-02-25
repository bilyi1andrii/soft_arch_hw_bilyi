FROM python:3.12-slim-bookworm
COPY --from=docker.io/astral/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY ./pyproject.toml ./uv.lock ./

RUN uv sync

COPY ./shared /app/shared
COPY ./facade /app/facade
COPY ./counter_service /app/counter_service
COPY ./logging_service /app/logging_service