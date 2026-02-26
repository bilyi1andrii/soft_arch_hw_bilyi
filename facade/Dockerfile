FROM python:3.12-slim-bookworm
COPY --from=docker.io/astral/uv:latest /uv /uvx /bin/

WORKDIR /app


COPY ./pyproject.toml ./uv.lock ./
RUN uv sync

COPY ./shared /app/shared
COPY ./facade /app/facade
COPY ./scripts/step3_demo.py /app/step3_demo.py
COPY ./scripts/step456_locks.py /app/step456_locks.py
COPY ./scripts/step8_queue.py /app/step8_queue.py


CMD ["uv", "run", "fastapi", "run", "facade/main.py", "--host", "0.0.0.0", "--port", "8000"]