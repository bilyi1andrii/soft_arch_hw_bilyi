FROM python:3.12-slim-bookworm
COPY --from=docker.io/astral/uv:latest /uv /uvx /bin/

WORKDIR /app


COPY ./pyproject.toml ./uv.lock ./
RUN uv sync


COPY ./shared /app/shared
COPY ./facade /app/facade

CMD ["uv", "run", "fastapi", "run", "facade/main.py", "--host", "0.0.0.0", "--port", "8000"]