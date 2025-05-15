#!/bin/bash

KAFKA_HOST="kafka"
KAFKA_PORT=9092

MINIO_HOST="minio"
MINIO_PORT=9000

TIMEOUT=60

wait_for_service() {
  local name=$1
  local host=$2
  local port=$3

  echo "🕐 Waiting for $name at $host:$port..."
  for i in $(seq 1 $TIMEOUT); do
    if timeout 1 bash -c "</dev/tcp/$host/$port" 2>/dev/null; then
      echo "$name is up!"
      return 0
    fi
    sleep 1
  done

  echo "❌ Timeout waiting for $name at $host:$port"
  exit 1
}

wait_for_service "Kafka" "$KAFKA_HOST" "$KAFKA_PORT"
wait_for_service "MinIO" "$MINIO_HOST" "$MINIO_PORT"

# All services are up, run the job
exec python /opt/main.py

