#!/usr/bin/env bash
# Deploy MirrorSourceConnector + ApicurioHeaderToConfluentWire SMT.
# This is the open (unlicensed) stand-in for Confluent Replicator + same SMT.
#
# PREFER_CONTENT_ID — transforms.headerToWire.prefer.content.id
#   false (default): use globalId header — matches Apicurio 2.x / this demo's producer
#   true:            use contentId header — set when producers used USE_ID=contentId
# PAYLOAD_ID_LENGTH — transforms.headerToWire.payload.id.length
#   4 (default) | 8 | auto
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
PREFER_CONTENT_ID="${PREFER_CONTENT_ID:-false}"
PAYLOAD_ID_LENGTH="${PAYLOAD_ID_LENGTH:-4}"
NAME="es-to-cflt-header-rewrite"

# Delete if already present
curl -sf -X DELETE "$CONNECT_URL/connectors/$NAME" >/dev/null 2>&1 || true
sleep 2

cat <<EOF | curl -sf -X POST "$CONNECT_URL/connectors" \
  -H 'Content-Type: application/json' \
  -d @-
{
  "name": "$NAME",
  "config": {
    "connector.class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
    "source.cluster.alias": "es",
    "target.cluster.alias": "cflt",
    "source.cluster.bootstrap.servers": "kafka-source:9092",
    "target.cluster.bootstrap.servers": "kafka-dest:9092",
    "topics": "orders",
    "replication.factor": "1",
    "sync.topic.acls.enabled": "false",
    "sync.topic.configs.enabled": "false",
    "emit.heartbeats.enabled": "false",
    "emit.checkpoints.enabled": "false",
    "refresh.topics.enabled": "false",
    "refresh.groups.enabled": "false",
    "offset-syncs.topic.replication.factor": "1",
    "heartbeats.topic.replication.factor": "1",
    "checkpoints.topic.replication.factor": "1",
    "replication.policy.class": "org.apache.kafka.connect.mirror.IdentityReplicationPolicy",
    "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "header.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "transforms": "headerToWire",
    "transforms.headerToWire.type": "io.ibm.migration.smt.ApicurioHeaderToConfluentWire\$Value",
    "transforms.headerToWire.prefer.content.id": "${PREFER_CONTENT_ID}",
    "transforms.headerToWire.payload.id.length": "${PAYLOAD_ID_LENGTH}",
    "transforms.headerToWire.strip.apicurio.headers": "true",
    "transforms.headerToWire.applies.to": "value"
  }
}
EOF

echo
echo "Connector '$NAME' created. Status:"
sleep 3
curl -sf "$CONNECT_URL/connectors/$NAME/status" | python3 -m json.tool 2>/dev/null \
  || curl -sf "$CONNECT_URL/connectors/$NAME/status"
echo
