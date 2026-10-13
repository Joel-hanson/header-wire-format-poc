#!/usr/bin/env bash
# Dump first record from source and dest to show header vs Confluent wire framing.
set -euo pipefail

echo "=== SOURCE (kafka-source / orders) — expect Apicurio headers, no magic byte ==="
docker exec kafka-source /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --max-messages 1 \
  --property print.headers=true \
  --property print.value=false \
  --timeout-ms 10000 || true

echo
echo "=== DEST (kafka-dest / orders) — after SMT: Confluent wire (magic 0x00 + 4-byte id) ==="
docker exec kafka-dest /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --max-messages 1 \
  --property print.headers=true \
  --property print.value=false \
  --timeout-ms 10000 || true

echo
echo "Tip: use 'consume-cflt' to fully deserialize with Confluent KafkaAvroDeserializer."
