#!/usr/bin/env bash
set -euo pipefail

echo "Creating source topic orders on kafka-source..."
docker exec kafka-source /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic orders --partitions 1 --replication-factor 1

echo "Creating source topic orders-new on kafka-source..."
docker exec kafka-source /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic orders-new --partitions 1 --replication-factor 1

echo "Creating dest topic orders on kafka-dest (Cluster Linking / MM2 IdentityReplicationPolicy keeps name)..."
docker exec kafka-dest /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic orders --partitions 1 --replication-factor 1

echo "Creating dest topic orders-new on kafka-dest..."
docker exec kafka-dest /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic orders-new --partitions 1 --replication-factor 1

echo "Topics ready."
