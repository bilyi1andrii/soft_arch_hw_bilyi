FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install fastapi uvicorn httpx

COPY shared /app/shared

COPY facade /app/facade

ENV PYTHONPATH=/app

CMD ["uvicorn", "facade.main:app", "--host", "0.0.0.0", "--port", "8000"]