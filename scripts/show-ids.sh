#!/usr/bin/env bash
# Show the key IDs for a subject side by side:
#   globalId / contentId (from Apicurio v2 API)
#   ccompat .id          (honours LEGACY_ID_MODE)
#   Confluent .id        (from Confluent Schema Registry)
#   raw header bytes on the source topic (first record, via inspect-records)
#
# Usage:
#   bash scripts/show-ids.sh [subject]
#
# Env:
#   SUBJECT            (default: orders-value)
#   APICURIO_CC        (default: http://localhost:8081/apis/ccompat/v7)
#   APICURIO_V2        (default: http://localhost:8081/apis/registry/v2)
#   CONFLUENT_SR       (default: http://localhost:8082)
set -euo pipefail

SUBJECT="${1:-${SUBJECT:-orders-value}}"
CC="${APICURIO_CC:-http://localhost:8081/apis/ccompat/v7}"
V2="${APICURIO_V2:-http://localhost:8081/apis/registry/v2}"
SR="${CONFLUENT_SR:-http://localhost:8082}"

echo "=== IDs for subject: $SUBJECT ==="
echo

echo "--- Apicurio /v2 (globalId + contentId per version) ---"
curl -sf "$V2/groups/default/artifacts/$SUBJECT/versions" \
  | python3 -c "
import json, sys
subject = sys.argv[1]
raw = sys.stdin.read().strip()
if not raw:
    print('  (no response from /v2 versions endpoint)')
    sys.exit(0)
data = json.loads(raw)
# API returns either a list directly or {\"versions\": [...]}
versions = data if isinstance(data, list) else data.get('versions', [])
for v in versions:
    print('  %-22s v%-3s  globalId=%-6s contentId=%s' % (
        subject, v['version'], v['globalId'], v.get('contentId', 'n/a')))
" "$SUBJECT"

echo
echo "--- Apicurio ccompat .id per version (honours LEGACY_ID_MODE) ---"
for v in $(curl -sf "$CC/subjects/$SUBJECT/versions" \
             | python3 -c 'import json,sys; print(" ".join(map(str,sorted(json.load(sys.stdin)))))'); do
  id=$(curl -sf "$CC/subjects/$SUBJECT/versions/$v" \
         | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
  echo "  $SUBJECT  v$v  ccompat_id=$id"
done

echo
echo "--- Confluent Schema Registry .id per version ---"
if curl -sf "$SR/subjects/$SUBJECT/versions" >/dev/null 2>&1; then
  for v in $(curl -sf "$SR/subjects/$SUBJECT/versions" \
               | python3 -c 'import json,sys; print(" ".join(map(str,sorted(json.load(sys.stdin)))))'); do
    id=$(curl -sf "$SR/subjects/$SUBJECT/versions/$v" \
           | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
    echo "  $SUBJECT  v$v  confluent_id=$id"
  done
else
  echo "  (subject not yet present in Confluent)"
fi

echo
echo "--- Raw Apicurio headers on first source record (kafka-source / orders topic) ---"
docker exec kafka-source /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --max-messages 1 \
  --property print.headers=true \
  --property print.value=false \
  --timeout-ms 10000 2>/dev/null \
| grep -v '^$' || echo "  (no records on source topic yet)"
