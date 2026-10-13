#!/usr/bin/env bash
# Export every subject/version from Apicurio ccompat and IMPORT it into Confluent
# with the SAME schema ID (explicit "id" in IMPORT mode). Mirrors the Event
# Streams schemas-ccompat-export + IMPORT flow.
#
# Asserts, per version: the ID Confluent returns == the Apicurio ccompat ID, and
# Confluent's /schemas/ids/{id} returns the same schema. Exits 1 on any mismatch.
#
# SUBJECTS="orders-value"  limits the run to the listed subjects (default: all).
set -euo pipefail

APICURIO_CC="${APICURIO_CC:-http://localhost:8081/apis/ccompat/v7}"
CONFLUENT_SR="${CONFLUENT_SR:-http://localhost:8082}"
CT='Content-Type: application/vnd.schemaregistry.v1+json'

subjects="${SUBJECTS:-$(curl -sf "$APICURIO_CC/subjects" | python3 -c 'import json,sys; print(" ".join(sorted(json.load(sys.stdin))))')}"
echo "Subjects to migrate: $subjects"

# Save original global compatibility, then set NONE for import tolerance.
orig_compat=$(curl -sf "$CONFLUENT_SR/config" | sed -n 's/.*"compatibilityLevel":"\([^"]*\)".*/\1/p' || true)
orig_compat=${orig_compat:-BACKWARD}
echo "Confluent original compatibility=$orig_compat"
curl -sf -X PUT "$CONFLUENT_SR/config" -H "$CT" -d '{"compatibility":"NONE"}' >/dev/null

failures=0
printf '%-20s %-4s %-10s %-12s %s\n' subject ver apicurio confluent result
for subject in $subjects; do
  curl -sf -X PUT "$CONFLUENT_SR/mode/$subject?force=true" -H "$CT" -d '{"mode":"IMPORT"}' >/dev/null

  for v in $(curl -sf "$APICURIO_CC/subjects/$subject/versions" | python3 -c 'import json,sys; print(" ".join(map(str, sorted(json.load(sys.stdin)))))'); do
    src=$(curl -sf "$APICURIO_CC/subjects/$subject/versions/$v")
    id=$(echo "$src" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
    # Explicit id + version, as IMPORT mode requires.
    payload=$(echo "$src" | python3 -c '
import json, sys
s = json.load(sys.stdin)
out = {"schema": s["schema"], "id": s["id"], "version": s["version"],
       "schemaType": s.get("schemaType", "AVRO")}
if s.get("references"):
    out["references"] = s["references"]
print(json.dumps(out))')

    resp=$(curl -s -X POST "$CONFLUENT_SR/subjects/$subject/versions" -H "$CT" -d "$payload")
    got=$(echo "$resp" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id", "ERR"))' 2>/dev/null || echo ERR)

    result="OK"
    if [[ "$got" != "$id" ]]; then
      result="FAIL: $resp"
    else
      # The schema Confluent now serves for this ID must equal the Apicurio one.
      same=$(python3 - "$APICURIO_CC/schemas/ids/$id" "$CONFLUENT_SR/schemas/ids/$id" <<'PY'
import json, sys, urllib.request
def schema(url):
    with urllib.request.urlopen(url) as r:
        return json.loads(json.loads(r.read())["schema"])
print("yes" if schema(sys.argv[1]) == schema(sys.argv[2]) else "no")
PY
)
      [[ "$same" == "yes" ]] || result="FAIL: schema behind id $id differs"
    fi
    [[ "$result" == OK ]] || failures=$((failures + 1))
    printf '%-20s %-4s %-10s %-12s %s\n' "$subject" "v$v" "$id" "$got" "$result"
  done

  curl -sf -X PUT "$CONFLUENT_SR/mode/$subject" -H "$CT" -d '{"mode":"READWRITE"}' >/dev/null
done

curl -sf -X PUT "$CONFLUENT_SR/config" -H "$CT" -d "{\"compatibility\":\"$orig_compat\"}" >/dev/null

if [[ $failures -gt 0 ]]; then
  echo "Schema migration FAILED: $failures version(s) did not keep their ID." >&2
  exit 1
fi
echo "Schema migration complete (all IDs preserved)."
