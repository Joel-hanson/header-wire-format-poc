#!/usr/bin/env bash
# Record-level check of the Confluent-side payloads (see PayloadIdVerifier.java).
# For every replicated record: magic byte 0x00, the 4-byte payload ID == the
# Apicurio header ID on the source record at the same offset, the Avro body is
# unchanged, and the ID resolves to the same schema in Confluent and Apicurio.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
APP_JAR=$(ls "$ROOT"/apps/target/header-replicator-apps-*.jar | grep -v original | head -1)

SOURCE_BOOTSTRAP="${SOURCE_BOOTSTRAP:-localhost:29092}" \
DEST_BOOTSTRAP="${DEST_BOOTSTRAP:-localhost:29094}" \
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8082}" \
APICURIO_CCOMPAT="${APICURIO_CCOMPAT:-http://localhost:8081/apis/ccompat/v7}" \
PREFER_CONTENT_ID="${PREFER_CONTENT_ID:-true}" \
  java -jar "$APP_JAR" verify-ids 2>&1 | grep -v '^SLF4J'
exit "${PIPESTATUS[0]}"
