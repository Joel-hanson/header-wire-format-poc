#!/usr/bin/env bash
# Phase 0b — Reproduce the ES upgrade path: Apicurio 2.x → 3.3.2
#
# Verifies that IDs (globalId, contentId, ccompat id) are preserved when
# a 2.x Apicurio registry is exported and imported into 3.3.2, and that
# records written against 2.x still decode against Confluent after the upgrade.
#
# Run for cases 1, 2, and 6 (one call per case number).
#
# Usage:
#   bash scripts/run-phase0b.sh <case-number>
#
# Env overrides (rarely needed):
#   ROOT   path to the header-replicator-poc directory (auto-detected)
set -euo pipefail

CASE="${1:?usage: $0 <case-number (1, 2, or 6)>}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

case "$CASE" in
  1) LEGACY=false; USE_ID=contentId; ENABLE_HEADERS=true;  ID_HANDLER="";    PREFER_CONTENT_ID=true  ;;
  2) LEGACY=true;  USE_ID=globalId;  ENABLE_HEADERS=true;  ID_HANDLER="";    PREFER_CONTENT_ID=false ;;
  6) LEGACY=false; USE_ID=contentId; ENABLE_HEADERS=false; ID_HANDLER="";    PREFER_CONTENT_ID=true  ;;
  *) echo "Phase 0b only defined for cases 1, 2, 6; got $CASE" >&2; exit 2 ;;
esac

APP_JAR=$(ls apps/target/header-replicator-apps-*.jar 2>/dev/null | grep -v original | head -1 || true)
if [[ -z "$APP_JAR" ]]; then
  echo "==> Building apps jar..."
  (cd apps && mvn -q -DskipTests package)
  APP_JAR=$(ls apps/target/header-replicator-apps-*.jar | grep -v original | head -1)
fi

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║  Phase 0b — Case $CASE — 2.x → 3.3.2 upgrade path                      ║"
printf "║  LEGACY=%-5s  USE_ID=%-9s  ENABLE_HEADERS=%-5s               ║\n" \
  "$LEGACY" "$USE_ID" "$ENABLE_HEADERS"
echo "╚══════════════════════════════════════════════════════════════════════╝"

# ── Step 1: Start Apicurio 2.6.13 (in-memory) alongside Kafka source ──────────
echo "==> Starting fresh stack with Apicurio 2.6.13.Final..."
docker-compose down -v --remove-orphans 2>/dev/null || true

# Override the apicurio image to 2.6.x using env override.
# docker-compose.yml uses image: apicurio/apicurio-registry:3.3.2.
# We override at runtime via COMPOSE_FILE override or docker run.
# Simplest: bring up only the non-apicurio services from compose, then start
# apicurio 2.6.x separately on the same port.

# Start Kafka and Confluent services (no Apicurio yet)
APICURIO_IMAGE=apicurio/apicurio-registry-mem:2.6.13.Final \
LEGACY_ID_MODE="$LEGACY" \
  docker-compose up -d kafka-source kafka-dest schema-registry connect 2>&1 | tail -5

# Start Apicurio 2.6.13 manually (health endpoint differs: /health/ready)
docker rm -f apicurio 2>/dev/null || true
docker run -d --name apicurio \
  --network "$(docker inspect kafka-source --format '{{range .NetworkSettings.Networks}}{{.NetworkID}}{{end}}' | head -1)" \
  -p 8081:8080 \
  -e "QUARKUS_PROFILE=prod" \
  -e "REGISTRY_CCOMPAT_LEGACY_ID_MODE_ENABLED=${LEGACY}" \
  apicurio/apicurio-registry-mem:2.6.13.Final

echo "==> Waiting for Apicurio 2.6.13 to be ready..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:8081/health/ready >/dev/null 2>&1; then
    echo "  Apicurio 2.6.13 ready."
    break
  fi
  sleep 2
  if [[ $i -eq 30 ]]; then
    echo "ERROR: Apicurio 2.6.13 did not start in time." >&2
    docker logs apicurio 2>&1 | tail -20
    exit 1
  fi
done

echo "==> Waiting for other services..."
bash scripts/wait-for.sh
bash scripts/create-topics.sh

# ── Step 2: Seed + produce "old" records against Apicurio 2.x ─────────────────
echo "==> Seeding edge-case schemas (pre phase on Apicurio 2.x)..."
bash scripts/seed-edge-case-schemas.sh pre

echo "==> Producing 5 records against Apicurio 2.x (USE_ID=$USE_ID ENABLE_HEADERS=$ENABLE_HEADERS)..."
SOURCE_BOOTSTRAP=localhost:29092 \
APICURIO_URL=http://localhost:8081 \
APICURIO_ENABLE_HEADERS="$ENABLE_HEADERS" \
APICURIO_USE_ID="$USE_ID" \
APICURIO_ID_HANDLER="$ID_HANDLER" \
  java -jar "$APP_JAR" produce 5 2>&1 | grep -v '^SLF4J'

# ── Step 3: Capture IDs before upgrade ────────────────────────────────────────
echo "==> Capturing IDs before upgrade (ids-before.txt)..."
bash scripts/show-ids.sh orders-value 2>&1 | tee /tmp/phase0b-case${CASE}-ids-before.txt

# ── Step 4: Export from Apicurio 2.6.13 ───────────────────────────────────────
echo "==> Exporting registry from Apicurio 2.x..."
curl -sf -o /tmp/apicurio-2x-export-case${CASE}.zip \
  "http://localhost:8081/apis/registry/v2/admin/export"
echo "  Export size: $(wc -c < /tmp/apicurio-2x-export-case${CASE}.zip) bytes"

# ── Step 5: Swap Apicurio container to 3.3.2 ──────────────────────────────────
echo "==> Swapping Apicurio to 3.3.2 (keeping Kafka + records)..."
docker rm -f apicurio

docker run -d --name apicurio \
  --network "$(docker inspect kafka-source --format '{{range .NetworkSettings.Networks}}{{.NetworkID}}{{end}}' | head -1)" \
  -p 8081:8080 \
  -e "APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED=${LEGACY}" \
  -e "APICURIO_REST_DELETION_ARTIFACT_ENABLED=true" \
  -e "APICURIO_REST_DELETION_ARTIFACT_VERSION_ENABLED=true" \
  -e "APICURIO_REST_DELETION_GROUP_ENABLED=true" \
  apicurio/apicurio-registry:3.3.2

echo "==> Waiting for Apicurio 3.3.2 to be ready..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:8081/apis/registry/v2/system/info >/dev/null 2>&1; then
    echo "  Apicurio 3.3.2 ready."
    break
  fi
  sleep 2
  if [[ $i -eq 30 ]]; then
    echo "ERROR: Apicurio 3.3.2 did not start after container swap." >&2
    docker logs apicurio 2>&1 | tail -20
    exit 1
  fi
done

# ── Step 6: Import the export into 3.3.2 ──────────────────────────────────────
echo "==> Importing 2.x export into Apicurio 3.3.2..."
import_http=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "http://localhost:8081/apis/registry/v3/admin/import" \
  -H "Content-Type: application/zip" \
  --data-binary @/tmp/apicurio-2x-export-case${CASE}.zip)
echo "  /v3/admin/import -> HTTP $import_http"

if [[ "$import_http" != "200" && "$import_http" != "201" && "$import_http" != "204" ]]; then
  echo "  /v3 import failed, trying /v2 admin import..."
  import_http=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "http://localhost:8081/apis/registry/v2/admin/import" \
    -H "Content-Type: application/zip" \
    --data-binary @/tmp/apicurio-2x-export-case${CASE}.zip)
  echo "  /v2/admin/import -> HTTP $import_http"
fi

if [[ "$import_http" != "200" && "$import_http" != "201" && "$import_http" != "204" ]]; then
  echo "ERROR: Import failed (HTTP $import_http)." >&2
  exit 1
fi

# ── Step 7: Capture IDs after upgrade ─────────────────────────────────────────
echo "==> Capturing IDs after upgrade (ids-after.txt)..."
bash scripts/show-ids.sh orders-value 2>&1 | tee /tmp/phase0b-case${CASE}-ids-after.txt

echo
echo "==> Diff ids-before vs ids-after (must be empty for success):"
if diff /tmp/phase0b-case${CASE}-ids-before.txt /tmp/phase0b-case${CASE}-ids-after.txt; then
  echo "  IDs UNCHANGED — 2.x IDs preserved in 3.3.2"
  ids_preserved=true
else
  echo "  WARNING: IDs changed after upgrade."
  ids_preserved=false
fi

# ── Step 8: Old records still decode on source (against 3.3.2) ────────────────
echo "==> Verifying old records decode against Apicurio 3.3.2..."
es_exit=0
SOURCE_BOOTSTRAP=localhost:29092 APICURIO_URL=http://localhost:8081 \
  java -jar "$APP_JAR" consume-es 2>&1 | grep -v '^SLF4J' || es_exit=$?

# ── Step 9: Produce new records after the upgrade ──────────────────────────────
echo "==> Producing 3 more records AFTER the 3.3.2 upgrade..."
SOURCE_BOOTSTRAP=localhost:29092 \
APICURIO_URL=http://localhost:8081 \
APICURIO_ENABLE_HEADERS="$ENABLE_HEADERS" \
APICURIO_USE_ID="$USE_ID" \
APICURIO_ID_HANDLER="$ID_HANDLER" \
  java -jar "$APP_JAR" produce 3 2>&1 | grep -v '^SLF4J'

# ── Step 10: Migrate schemas + run connector ───────────────────────────────────
echo "==> Migrating schemas to Confluent (from 3.3.2 ccompat)..."
SUBJECTS="orders-value" bash scripts/migrate-schema.sh

echo "==> Starting connector (PREFER_CONTENT_ID=$PREFER_CONTENT_ID)..."
PREFER_CONTENT_ID="$PREFER_CONTENT_ID" bash scripts/start-connector.sh
echo "==> Waiting for replication (25s)..."
sleep 25

# ── Step 11: Verify all records (old + new) ────────────────────────────────────
EXPECTED_ID_SOURCE="$( [[ "$ENABLE_HEADERS" == "true" ]] && echo headers || echo payload8 )"
echo "==> Running payload ID check (EXPECTED_ID_SOURCE=$EXPECTED_ID_SOURCE)..."
verify_exit=0
PREFER_CONTENT_ID="$PREFER_CONTENT_ID" \
EXPECTED_ID_SOURCE="$EXPECTED_ID_SOURCE" \
  bash scripts/verify-payload-ids.sh || verify_exit=$?

echo "==> Consuming ALL records from Confluent (old + new)..."
consumer_exit=0
DEST_BOOTSTRAP=localhost:29094 SCHEMA_REGISTRY_URL=http://localhost:8082 \
  java -jar "$APP_JAR" consume-cflt 2>&1 | grep -v '^SLF4J' || consumer_exit=$?

# ── Verdict ────────────────────────────────────────────────────────────────────
echo
echo "══════════════════════════════════════════════════════════════════════"
echo "  Phase 0b — Case $CASE result:"
echo "  ids_preserved=$ids_preserved"
echo "  old-records-decode exit=$es_exit"
echo "  verify-payload-ids exit=$verify_exit  consume-cflt exit=$consumer_exit"
echo

if [[ "$ids_preserved" == "true" && $verify_exit -eq 0 && $consumer_exit -eq 0 ]]; then
  echo "  RESULT: PASS — 2.x IDs preserved in 3.3.2; old+new records decode in Confluent."
else
  echo "  RESULT: FAIL"
  [[ "$ids_preserved" != "true" ]] && echo "    - IDs changed after upgrade"
  [[ $verify_exit -ne 0 ]] && echo "    - verify-payload-ids failed (exit=$verify_exit)"
  [[ $consumer_exit -ne 0 ]] && echo "    - consume-cflt failed (exit=$consumer_exit)"
fi
echo "══════════════════════════════════════════════════════════════════════"
