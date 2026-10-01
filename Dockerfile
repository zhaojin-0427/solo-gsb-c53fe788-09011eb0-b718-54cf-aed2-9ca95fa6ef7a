FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/data \
    DRILLS_DIR=/drills \
    STATIC_DIR=/app/frontend

WORKDIR /app

COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

COPY backend /app/backend
COPY frontend /app/frontend
COPY drills /drills

RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# Single process: the engine relies on one in-process writer lock and an
# in-memory connection hub. Scale horizontally only behind a sticky-session
# design or run one replica.
CMD ["uvicorn", "app.main:app", "--app-dir", "/app/backend", "--host", "0.0.0.0", "--port", "8000"]
