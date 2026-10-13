#!/usr/bin/env bash
set -euo pipefail

# Wait until a Kafka broker is reachable and its advertised listener is up by
# verifying that kafka-topics.sh can list topics without error.
wait_kafka() {
  local name="$1" container="$2" attempts="${3:-60}"
  echo "Waiting for $name Kafka broker ($container) ..."
  for i in $(seq 1 "$attempts"); do
    if docker exec "$container" \
        /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
      echo "$name broker is ready"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: $name broker not ready after $attempts attempts" >&2
  return 1
}

wait_http() {
  local name="$1" url="$2" attempts="${3:-60}"
  echo "Waiting for $name at $url ..."
  for i in $(seq 1 "$attempts"); do
    if curl -sf "$url" >/dev/null; then
      echo "$name is ready"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: $name not ready after $attempts attempts" >&2
  return 1
}

# Brokers must be ready before HTTP services that depend on them start accepting
# connections, and before create-topics.sh or the producer jar runs.
wait_kafka "Source" "kafka-source"
wait_kafka "Dest"   "kafka-dest"
wait_http "Apicurio"     "http://localhost:8081/apis/registry/v2/system/info"
wait_http "Confluent SR" "http://localhost:8082/subjects"
wait_http "Connect"      "http://localhost:8083/connectors"
echo "All services ready."
