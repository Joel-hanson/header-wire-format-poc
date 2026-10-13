#!/usr/bin/env bash
# Run one matrix case from the Part B test plan end-to-end from a clean stack.
# Prints PASS or FAIL for the case and a summary table at the end.
#
# Usage:
#   bash scripts/run-case.sh <case-number>
#
# Cases (from TEST-PLAN-8byte-and-legacy-id.md Phase 1):
#   1  legacy=false  headers   USE_ID=contentId  PREFER_CONTENT_ID=true   expect PASS
#   2  legacy=true   headers   USE_ID=globalId   PREFER_CONTENT_ID=false  expect PASS
#   3  legacy=true   headers   USE_ID=contentId  PREFER_CONTENT_ID=true   expect FAIL
#   4  legacy=false  headers   USE_ID=globalId   PREFER_CONTENT_ID=false  expect FAIL
#   5  legacy=true   headers   USE_ID=globalId   PREFER_CONTENT_ID=true   expect PASS (SMT fallback)
#   6  legacy=false  payload8  USE_ID=contentId  n/a                      expect FAIL (known gap)
#   7  legacy=false  payload4  USE_ID=contentId  n/a                      expect PASS
#   8  legacy=true   payload4  USE_ID=globalId   n/a                      expect PASS
#
# Cases for 2.x clients on a 3.x server with DEFAULT serde config (nothing set explicitly):
#   6b legacy=false  payload8  SMT payload.id.length=8                    expect PASS
#   9  legacy=false  mixed     SMT payload.id.length=auto                 expect PASS
#   10 legacy=false  mixed     SMT payload.id.length=8  (negative)        expect FAIL
#   11 legacy=false  mixed     SMT payload.id.length=4  (current SMT)     expect FAIL
#   12 legacy=false  serde defaults (headers, USE_ID=globalId), diverged subject   expect FAIL
#   13 legacy=true   serde defaults (headers, USE_ID=globalId), diverged subject   expect PASS
# "mixed" = 5 records written by the 2.x serde default handler (0x00 + 8-byte ID), then 5 records
# written with Legacy4ByteIdHandler (0x00 + 4-byte ID, the same bytes the 3.x Default4ByteIdHandler writes).
#
# ID divergence strategy
# ─────────────────────
# orders-value v1 always has globalId=contentId (it's the first unique schema after the gaps).
# orders-copy-value (registered in the mid-phase with byte-identical content to orders-value v1)
# gets a NEW globalId but REUSES orders-value v1's contentId.  So:
#   orders-value  v1:  globalId=5  contentId=5
#   orders-copy   v1:  globalId=6  contentId=5   <── diverges
#
# Cases 3 and 4 rely on this divergence:
#   Case 3: LEGACY_ID_MODE=true → ccompat.id = globalId.
#            Produce with USE_ID=contentId → header carries contentId=5.
#            migrate-schema imports orders-copy-value with id=globalId=6.
#            SMT puts payload-id=5 (from contentId header). Confluent has id=6 for this schema.
#            Verifier: payload-id 5 != globalId 6 → FAIL (or 404 if id=5 is a different schema).
#   Case 4: LEGACY_ID_MODE=false → ccompat.id = contentId=5.
#            Produce with USE_ID=globalId → header carries globalId=6.
#            migrate-schema imports orders-copy-value with id=contentId=5.
#            SMT puts payload-id=6 (from globalId header). Confluent has id=5 for this schema.
#            Verifier: payload-id 6 != expected 5 → FAIL.
#
# For cases 1/2/5 the test subject is orders-value (globalId=contentId), which correctly
# passes regardless of which ID the SMT picks (they're the same value).
# The diverging subject is orders-copy-value.
#
# Env overrides (rarely needed):
#   ROOT        path to the header-replicator-poc directory (auto-detected)
set -euo pipefail

CASE="${1:?usage: $0 <case-number>}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ── Case definitions ─────────────────────────────────────────────────────────
# APICURIO_SUBJECT: non-empty means produce under that subject (artifact ID override).
# Cases 3 and 4 use orders-copy-value which has globalId≠contentId after mid-phase seeding.
declare -A LEGACY USE_ID ENABLE_HEADERS ID_HANDLER PREFER_CONTENT_ID EXPECTED_ID_SOURCE EXPECT_PASS APICURIO_SUBJECT_OVERRIDE MIGRATE_SUBJECT PAYLOAD_ID_LENGTH MIXED_EXTRA_HANDLER
LEGACY[1]=false;  USE_ID[1]=contentId; ENABLE_HEADERS[1]=true;  ID_HANDLER[1]="";    PREFER_CONTENT_ID[1]=true;  EXPECTED_ID_SOURCE[1]=headers;  EXPECT_PASS[1]=true;  APICURIO_SUBJECT_OVERRIDE[1]="";                  MIGRATE_SUBJECT[1]=orders-value
LEGACY[2]=true;   USE_ID[2]=globalId;  ENABLE_HEADERS[2]=true;  ID_HANDLER[2]="";    PREFER_CONTENT_ID[2]=false; EXPECTED_ID_SOURCE[2]=headers;  EXPECT_PASS[2]=true;  APICURIO_SUBJECT_OVERRIDE[2]="";                  MIGRATE_SUBJECT[2]=orders-value
LEGACY[3]=true;   USE_ID[3]=contentId; ENABLE_HEADERS[3]=true;  ID_HANDLER[3]="";    PREFER_CONTENT_ID[3]=true;  EXPECTED_ID_SOURCE[3]=headers;  EXPECT_PASS[3]=false; APICURIO_SUBJECT_OVERRIDE[3]=orders-copy-value;  MIGRATE_SUBJECT[3]=orders-copy-value
LEGACY[4]=false;  USE_ID[4]=globalId;  ENABLE_HEADERS[4]=true;  ID_HANDLER[4]="";    PREFER_CONTENT_ID[4]=false; EXPECTED_ID_SOURCE[4]=headers;  EXPECT_PASS[4]=false; APICURIO_SUBJECT_OVERRIDE[4]=orders-copy-value;  MIGRATE_SUBJECT[4]=orders-copy-value
LEGACY[5]=true;   USE_ID[5]=globalId;  ENABLE_HEADERS[5]=true;  ID_HANDLER[5]="";    PREFER_CONTENT_ID[5]=true;  EXPECTED_ID_SOURCE[5]=headers;  EXPECT_PASS[5]=true;  APICURIO_SUBJECT_OVERRIDE[5]="";                  MIGRATE_SUBJECT[5]=orders-value
LEGACY[6]=false;  USE_ID[6]=contentId; ENABLE_HEADERS[6]=false; ID_HANDLER[6]="";    PREFER_CONTENT_ID[6]=true;  EXPECTED_ID_SOURCE[6]=payload8; EXPECT_PASS[6]=false; APICURIO_SUBJECT_OVERRIDE[6]="";                  MIGRATE_SUBJECT[6]=orders-value
LEGACY[7]=false;  USE_ID[7]=contentId; ENABLE_HEADERS[7]=false; ID_HANDLER[7]="io.apicurio.registry.serde.Legacy4ByteIdHandler"; PREFER_CONTENT_ID[7]=true; EXPECTED_ID_SOURCE[7]=payload4; EXPECT_PASS[7]=true; APICURIO_SUBJECT_OVERRIDE[7]=""; MIGRATE_SUBJECT[7]=orders-value
LEGACY[8]=true;   USE_ID[8]=globalId;  ENABLE_HEADERS[8]=false; ID_HANDLER[8]="io.apicurio.registry.serde.Legacy4ByteIdHandler"; PREFER_CONTENT_ID[8]=false; EXPECTED_ID_SOURCE[8]=payload4; EXPECT_PASS[8]=true; APICURIO_SUBJECT_OVERRIDE[8]=""; MIGRATE_SUBJECT[8]=orders-value
# 6b: same producer as case 6 (2.x default handler, 8-byte), SMT told the topic is 8-byte.
LEGACY[61]=false; USE_ID[61]=contentId; ENABLE_HEADERS[61]=false; ID_HANDLER[61]=""; PREFER_CONTENT_ID[61]=true; EXPECTED_ID_SOURCE[61]=payload8; EXPECT_PASS[61]=true; APICURIO_SUBJECT_OVERRIDE[61]=""; MIGRATE_SUBJECT[61]=orders-value; PAYLOAD_ID_LENGTH[61]=8
# 9-11: mixed topic, three SMT settings.
LEGACY[9]=false;  USE_ID[9]=contentId;  ENABLE_HEADERS[9]=false;  ID_HANDLER[9]="";  PREFER_CONTENT_ID[9]=true;  EXPECTED_ID_SOURCE[9]=payloadmixed;  EXPECT_PASS[9]=true;  APICURIO_SUBJECT_OVERRIDE[9]="";  MIGRATE_SUBJECT[9]=orders-value;  PAYLOAD_ID_LENGTH[9]=auto; MIXED_EXTRA_HANDLER[9]="io.apicurio.registry.serde.Legacy4ByteIdHandler"
LEGACY[10]=false; USE_ID[10]=contentId; ENABLE_HEADERS[10]=false; ID_HANDLER[10]=""; PREFER_CONTENT_ID[10]=true; EXPECTED_ID_SOURCE[10]=payloadmixed; EXPECT_PASS[10]=false; APICURIO_SUBJECT_OVERRIDE[10]=""; MIGRATE_SUBJECT[10]=orders-value; PAYLOAD_ID_LENGTH[10]=8;    MIXED_EXTRA_HANDLER[10]="io.apicurio.registry.serde.Legacy4ByteIdHandler"
LEGACY[11]=false; USE_ID[11]=contentId; ENABLE_HEADERS[11]=false; ID_HANDLER[11]=""; PREFER_CONTENT_ID[11]=true; EXPECTED_ID_SOURCE[11]=payloadmixed; EXPECT_PASS[11]=false; APICURIO_SUBJECT_OVERRIDE[11]=""; MIGRATE_SUBJECT[11]=orders-value; PAYLOAD_ID_LENGTH[11]=4;    MIXED_EXTRA_HANDLER[11]="io.apicurio.registry.serde.Legacy4ByteIdHandler"
# 12-13: 2.6.5 serde with NOTHING set: ENABLE_HEADERS=true, USE_ID=globalId, DefaultIdHandler.
# Diverged subject orders-copy-value (globalId=6, contentId=5) shows which ID Confluent needs.
LEGACY[12]=false; USE_ID[12]=default; ENABLE_HEADERS[12]=default; ID_HANDLER[12]=""; PREFER_CONTENT_ID[12]=true;  EXPECTED_ID_SOURCE[12]=headers; EXPECT_PASS[12]=false; APICURIO_SUBJECT_OVERRIDE[12]=orders-copy-value; MIGRATE_SUBJECT[12]=orders-copy-value
LEGACY[13]=true;  USE_ID[13]=default; ENABLE_HEADERS[13]=default; ID_HANDLER[13]=""; PREFER_CONTENT_ID[13]=false; EXPECTED_ID_SOURCE[13]=headers; EXPECT_PASS[13]=true;  APICURIO_SUBJECT_OVERRIDE[13]=orders-copy-value; MIGRATE_SUBJECT[13]="orders-value orders-copy-value"

# Accept "6b" as an alias for the internal key 61.
[[ "$CASE" == "6b" ]] && CASE=61
if [[ -z "${LEGACY[$CASE]+x}" ]]; then
  echo "Unknown case $CASE. Valid cases: 1-13 and 6b" >&2; exit 2
fi

legacy="${LEGACY[$CASE]}"
use_id="${USE_ID[$CASE]}"
enable_headers="${ENABLE_HEADERS[$CASE]}"
id_handler="${ID_HANDLER[$CASE]}"
prefer_content_id="${PREFER_CONTENT_ID[$CASE]}"
expected_id_source="${EXPECTED_ID_SOURCE[$CASE]}"
expect_pass="${EXPECT_PASS[$CASE]}"
apicurio_subject="${APICURIO_SUBJECT_OVERRIDE[$CASE]}"
migrate_subject="${MIGRATE_SUBJECT[$CASE]}"
payload_id_length="${PAYLOAD_ID_LENGTH[$CASE]:-4}"
mixed_extra_handler="${MIXED_EXTRA_HANDLER[$CASE]:-}"

echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║  Case $CASE                                                              ║"
echo "╠══════════════════════════════════════════════════════════════════════╣"
printf "║  LEGACY_ID_MODE=%-6s  APICURIO_USE_ID=%-10s  ENABLE_HEADERS=%-5s ║\n" \
  "$legacy" "$use_id" "$enable_headers"
printf "║  PREFER_CONTENT_ID=%-5s  EXPECTED_ID_SOURCE=%-10s  EXPECT=%-4s ║\n" \
  "$prefer_content_id" "$expected_id_source" "$([ "$expect_pass" = true ] && echo PASS || echo FAIL)"
echo "╚══════════════════════════════════════════════════════════════════════╝"
echo

cd "$ROOT"

# ── 0. Build app jar ─────────────────────────────────────────────────────────
APP_JAR=$(ls apps/target/header-replicator-apps-*.jar 2>/dev/null | grep -v original | head -1 || true)
if [[ -z "$APP_JAR" ]]; then
  echo "==> Building apps jar..."
  (cd apps && mvn -q -DskipTests package)
  APP_JAR=$(ls apps/target/header-replicator-apps-*.jar | grep -v original | head -1)
fi

# ── 1. Clean stack ────────────────────────────────────────────────────────────
echo "==> Tearing down previous stack..."
docker-compose down -v --remove-orphans 2>/dev/null || true

echo "==> Starting stack (LEGACY_ID_MODE=$legacy)..."
LEGACY_ID_MODE="$legacy" docker-compose up -d --build

echo "==> Waiting for services..."
bash scripts/wait-for.sh

echo "==> Creating topics..."
bash scripts/create-topics.sh

# ── 2. Seed pre-phase ────────────────────────────────────────────────────────
echo "==> Seeding edge-case schemas (pre — gaps + lookalikes)..."
bash scripts/seed-edge-case-schemas.sh pre

# ── 3. Produce ────────────────────────────────────────────────────────────────
# For cases 3 and 4, APICURIO_SUBJECT overrides the artifact resolver to use
# orders-copy-value (globalId≠contentId); records still land on the orders topic.
# The mid-phase seeding copies orders-value v1's schema — so orders-value must exist
# first. We produce one seed record under orders-value (default TopicIdStrategy),
# then seed mid (which creates orders-copy-value), then produce the 5 test records
# under orders-copy-value.
if [[ -n "$apicurio_subject" ]]; then
  # Step 3a: register orders-value by producing 1 record under the normal subject.
  echo "==> [Case $CASE] Seeding orders-value schema (1 record under default subject)..."
  SOURCE_BOOTSTRAP=localhost:29092 \
  APICURIO_URL=http://localhost:8081 \
  APICURIO_ENABLE_HEADERS="$enable_headers" \
  APICURIO_USE_ID="$use_id" \
  APICURIO_ID_HANDLER="$id_handler" \
    java -jar "$APP_JAR" produce 1

  # Step 3b: seed mid now that orders-value exists.
  echo "==> [Case $CASE] Seeding mid-phase (creates orders-copy-value with globalId≠contentId)..."
  bash scripts/seed-edge-case-schemas.sh mid
  mid_done=true

  # Step 3c: produce the 5 test records under the diverging subject.
  echo "==> Producing 5 records via subject=$apicurio_subject (USE_ID=$use_id ENABLE_HEADERS=$enable_headers)..."
  SOURCE_BOOTSTRAP=localhost:29092 \
  APICURIO_URL=http://localhost:8081 \
  APICURIO_ENABLE_HEADERS="$enable_headers" \
  APICURIO_USE_ID="$use_id" \
  APICURIO_ID_HANDLER="$id_handler" \
  APICURIO_SUBJECT="$apicurio_subject" \
    java -jar "$APP_JAR" produce 5
else
  mid_done=false
  echo "==> Producing 5 records (USE_ID=$use_id ENABLE_HEADERS=$enable_headers)..."
  SOURCE_BOOTSTRAP=localhost:29092 \
  APICURIO_URL=http://localhost:8081 \
  APICURIO_ENABLE_HEADERS="$enable_headers" \
  APICURIO_USE_ID="$use_id" \
  APICURIO_ID_HANDLER="$id_handler" \
    java -jar "$APP_JAR" produce 5
  if [[ -n "$mixed_extra_handler" ]]; then
    # Mixed topic: a second batch with a different ID handler on the same topic and subject.
    # Legacy4ByteIdHandler writes 0x00 + 4-byte int, the same bytes as the 3.x Default4ByteIdHandler.
    echo "==> [Case $CASE] Producing 5 more records with ID_HANDLER=$mixed_extra_handler (mixed topic)..."
    SOURCE_BOOTSTRAP=localhost:29092 \
    APICURIO_URL=http://localhost:8081 \
    APICURIO_ENABLE_HEADERS="$enable_headers" \
    APICURIO_USE_ID="$use_id" \
    APICURIO_ID_HANDLER="$mixed_extra_handler" \
      java -jar "$APP_JAR" produce 5
  fi
fi

# ── 4. Seed mid-phase — creates orders-copy-value (globalId != contentId) ────
if [[ "$mid_done" == "true" ]]; then
  echo "==> [Case $CASE] mid-phase already seeded (done before produce step)."
else
  echo "==> Seeding edge-case schemas (mid — orders-copy-value creates globalId/contentId split)..."
  bash scripts/seed-edge-case-schemas.sh mid
fi

# ── 5. Show IDs and confirm divergence ───────────────────────────────────────
echo "==> ID table (Phase 0 exit check)..."
bash scripts/show-ids.sh orders-value
bash scripts/show-ids.sh orders-copy-value 2>/dev/null || true

# Verify divergence for orders-copy-value (this is the subject we test for cases 3/4).
COPY_V2=$(curl -sf "http://localhost:8081/apis/registry/v2/groups/default/artifacts/orders-copy-value/versions" \
  | python3 -c "import json,sys; vs=json.loads(sys.stdin.read()).get('versions',[]); v=vs[0] if vs else {}; print(v.get('globalId',''),v.get('contentId',''))" 2>/dev/null || echo "- -")
COPY_GID=$(echo "$COPY_V2" | awk '{print $1}')
COPY_CID=$(echo "$COPY_V2" | awk '{print $2}')
echo "  orders-copy-value v1: globalId=$COPY_GID contentId=$COPY_CID"

if [[ "$COPY_GID" != "$COPY_CID" ]]; then
  echo "  ID divergence confirmed: globalId=$COPY_GID != contentId=$COPY_CID"
else
  echo "  NOTE: orders-copy-value globalId==contentId — Apicurio is not sharing content hashes."
  echo "  Cases 3 and 4 may not produce the expected mismatch."
fi

# ── 6. Source consumer sanity check ──────────────────────────────────────────
echo "==> Verifying source Apicurio consumer can read..."
SOURCE_BOOTSTRAP=localhost:29092 APICURIO_URL=http://localhost:8081 \
  java -jar "$APP_JAR" consume-es || true

# ── 7. Migrate schemas ────────────────────────────────────────────────────────
# For cases 3/4: migrate orders-copy-value (the subject the producer used). Its ccompat id
# will be globalId=6 (legacy=true) or contentId=5 (legacy=false). The topic records carry
# the OTHER id, so Confluent ends up with the wrong id for those payloads → expected FAIL.
# For all other cases: migrate orders-value as usual.
echo "==> Migrating schemas: SUBJECTS=$migrate_subject (ccompat -> Confluent IMPORT)..."
SUBJECTS="$migrate_subject" bash scripts/migrate-schema.sh

# ── 8. Start connector with matching PREFER_CONTENT_ID ───────────────────────
echo "==> Starting connector (PREFER_CONTENT_ID=$prefer_content_id PAYLOAD_ID_LENGTH=$payload_id_length)..."
PREFER_CONTENT_ID="$prefer_content_id" PAYLOAD_ID_LENGTH="$payload_id_length" bash scripts/start-connector.sh

echo "==> Waiting for replication..."
sleep 15
bash scripts/inspect-records.sh || true

# ── 9. Payload ID check ───────────────────────────────────────────────────────
echo "==> Running payload ID check..."
verify_exit=0
PREFER_CONTENT_ID="$prefer_content_id" \
EXPECTED_ID_SOURCE="$expected_id_source" \
  bash scripts/verify-payload-ids.sh || verify_exit=$?

# ── 10. Confluent consumer ────────────────────────────────────────────────────
echo "==> Consuming from Confluent..."
consumer_exit=0
DEST_BOOTSTRAP=localhost:29094 SCHEMA_REGISTRY_URL=http://localhost:8082 \
  java -jar "$APP_JAR" consume-cflt || consumer_exit=$?

# ── 11. Verdict ───────────────────────────────────────────────────────────────
echo
echo "══════════════════════════════════════════════════════════════════════"
echo "  Case $CASE result:"
echo "  verify-payload-ids exit=$verify_exit  consume-cflt exit=$consumer_exit"
echo "  Expected: $([ "$expect_pass" = true ] && echo PASS || echo FAIL)"
echo

overall_pass=false
if [[ $verify_exit -eq 0 && $consumer_exit -eq 0 ]]; then
  overall_pass=true
fi

if [[ "$overall_pass" = true && "$expect_pass" = true ]]; then
  echo "  RESULT: PASS  (expected PASS, got PASS)"
elif [[ "$overall_pass" = false && "$expect_pass" = false ]]; then
  echo "  RESULT: PASS  (expected FAIL, got FAIL — known gap confirmed)"
elif [[ "$overall_pass" = true && "$expect_pass" = false ]]; then
  echo "  RESULT: UNEXPECTED PASS — IDs may not have diverged (see show-ids output above)"
elif [[ "$overall_pass" = false && "$expect_pass" = true ]]; then
  echo "  RESULT: UNEXPECTED FAIL — case should have passed but did not"
fi
echo "══════════════════════════════════════════════════════════════════════"
