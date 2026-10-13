#!/usr/bin/env bash
# Negative checks on Confluent IMPORT mode (observed on cp-schema-registry 7.7.1).
# Run after migrate-schema.sh. Uses throwaway subjects and cleans them up.
#
#   1. Re-importing a migrated schema with its own ID under a new subject -> accepted, same ID.
#   2. Importing a DIFFERENT schema with an already-used ID                 -> rejected (42205).
#   3. Importing a migrated schema under a NEW, unused ID                   -> accepted with the
#      new ID. The SAME schema then has two IDs in Confluent; the original
#      ID still resolves, so existing payloads are unaffected.
set -euo pipefail

SR="${CONFLUENT_SR:-http://localhost:8082}"
CT='Content-Type: application/vnd.schemaregistry.v1+json'
SUBJECT="${SUBJECT:-orders-value}"
fail=0

body() {  # schema-json, id
  python3 -c 'import json,sys; print(json.dumps({"schema": sys.argv[1], "id": int(sys.argv[2]), "version": 1, "schemaType": "AVRO"}))' "$1" "$2"
}
import_as() {  # subject, schema, id -> prints response
  curl -sf -X PUT "$SR/mode/$1?force=true" -H "$CT" -d '{"mode":"IMPORT"}' >/dev/null
  curl -s -X POST "$SR/subjects/$1/versions" -H "$CT" -d "$(body "$2" "$3")"
}
cleanup() {
  for s in import-check-same-value import-check-conflict-value import-check-newid-value; do
    curl -s -X DELETE "$SR/subjects/$s" >/dev/null 2>&1 || true
    curl -s -X DELETE "$SR/subjects/$s?permanent=true" >/dev/null 2>&1 || true
    curl -s -X DELETE "$SR/mode/$s" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

latest=$(curl -sf "$SR/subjects/$SUBJECT/versions/latest")
id=$(echo "$latest" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
schema=$(echo "$latest" | python3 -c 'import json,sys; print(json.load(sys.stdin)["schema"])')
max_id=$(curl -sf "$SR/schemas?deleted=true" | python3 -c 'import json,sys; print(max(s["id"] for s in json.load(sys.stdin)))')
echo "Using $SUBJECT latest: id=$id  (highest id in Confluent: $max_id)"

r=$(import_as import-check-same-value "$schema" "$id")
if [[ "$(echo "$r" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id"))')" == "$id" ]]; then
  echo "  OK   1. same schema + same id under new subject -> accepted (id=$id)"
else
  echo "  FAIL 1. expected id=$id, got: $r"; fail=1
fi

other='{"type":"record","name":"Conflict","fields":[{"name":"z","type":"string"}]}'
r=$(import_as import-check-conflict-value "$other" "$id")
if echo "$r" | grep -q '42205'; then
  echo "  OK   2. different schema + taken id -> rejected: $r"
else
  echo "  FAIL 2. Confluent did not reject an ID collision: $r"; fail=1
fi

new_id=$((max_id + 1000))
r=$(import_as import-check-newid-value "$schema" "$new_id")
if [[ "$(echo "$r" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id"))')" == "$new_id" ]] \
   && curl -sf "$SR/schemas/ids/$id" >/dev/null; then
  echo "  OK   3. same schema + new id $new_id -> accepted; original id $id still resolves"
else
  echo "  FAIL 3. got: $r"; fail=1
fi

[[ $fail -eq 0 ]] && echo "IMPORT-mode checks PASSED." || { echo "IMPORT-mode checks FAILED." >&2; exit 1; }
