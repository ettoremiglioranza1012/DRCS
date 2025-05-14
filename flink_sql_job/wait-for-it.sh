#!/bin/bash

HOST="kafka"
PORT=9092
TIMEOUT=60

echo "Waiting for Kafka at $HOST:$PORT..."
for i in $(seq 1 $TIMEOUT); do
  if timeout 1 bash -c "</dev/tcp/$HOST/$PORT" 2>/dev/null; then
    echo "Kafka is up!"
    exec /opt/flink/bin/sql-client.sh embedded -f /opt/flink/job.sql
  fi
  sleep 1
done

echo "Timeout waiting for Kafka at $HOST:$PORT"
exit 1

