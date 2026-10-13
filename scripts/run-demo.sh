#!/usr/bin/env bash
# EDGE_CASES=1 adds lookalike schemas, a v2 evolution and hard deletes so schema
# IDs are not contiguous, then checks every Confluent payload ID record by record.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
EDGE_CASES="${EDGE_CASES:-0}"

echo "==> 1. Start infrastructure"
docker-compose up -d --build

echo "==> 2. Wait for services"
bash scripts/wait-for.sh

echo "==> 3. Create topics"
bash scripts/create-topics.sh

echo "==> 4. Build client apps"
(cd apps && mvn -q -DskipTests package)
APP_JAR=$(ls apps/target/header-replicator-apps-*.jar | grep -v original | head -1)

if [[ "$EDGE_CASES" == "1" ]]; then
  echo "==> 4b. Seed edge-case schemas (lookalikes + hard deletes -> ID gaps)"
  bash scripts/seed-edge-case-schemas.sh pre
fi

echo "==> 5. Produce header-format records to source (ES stand-in)"
SOURCE_BOOTSTRAP=localhost:29092 APICURIO_URL=http://localhost:8081 \
  java -jar "$APP_JAR" produce 5

if [[ "$EDGE_CASES" == "1" ]]; then
  echo "==> 5b. More edge-case schemas, then produce with evolved schema v2 (new ID)"
  bash scripts/seed-edge-case-schemas.sh mid
  SOURCE_BOOTSTRAP=localhost:29092 APICURIO_URL=http://localhost:8081 \
    java -jar "$APP_JAR" produce-v2 3
fi

echo "==> 6. Verify Apicurio consumer can read source"
SOURCE_BOOTSTRAP=localhost:29092 APICURIO_URL=http://localhost:8081 \
  java -jar "$APP_JAR" consume-es

echo "==> 7. Migrate schemas (ccompat export → Confluent IMPORT, IDs preserved)"
bash scripts/migrate-schema.sh

echo "==> 8. Start MirrorSourceConnector + header→wire SMT"
bash scripts/start-connector.sh

echo "==> 9. Wait for replication"
sleep 15
bash scripts/inspect-records.sh || true

echo "==> 9b. Offset continuity regression"
bash scripts/verify-offset-continuity.sh

echo "==> 9c. Confluent payload ID check (record by record)"
bash scripts/verify-payload-ids.sh

if [[ "$EDGE_CASES" == "1" ]]; then
  echo "==> 9d. Confluent IMPORT-mode negative checks"
  bash scripts/verify-import-mode.sh
fi

echo "==> 10. Consume from Confluent with standard KafkaAvroDeserializer (no Apicurio)"
DEST_BOOTSTRAP=localhost:29094 SCHEMA_REGISTRY_URL=http://localhost:8082 \
  java -jar "$APP_JAR" consume-cflt

echo
echo "Demo complete."
