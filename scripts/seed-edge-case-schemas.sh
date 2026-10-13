#!/usr/bin/env bash
# Seed Apicurio with edge-case schemas so schema IDs are NOT contiguous and
# contentId != globalId. Run it in two phases around the producer:
#
#   seed-edge-case-schemas.sh pre   # before the first `produce`
#   seed-edge-case-schemas.sh mid   # after `produce`, before `produce-v2`
#
# Apicurio 3.3.2 behaviour seen in quick manual probes:
#   - contentId is shared by identical content across subjects; globalId is per version.
#   - contentId and globalId appear to draw from ONE counter, so they diverge as soon
#     as identical content is registered twice.
#   - After a hard delete, NEW content gets a higher ID (gap). Re-registering the SAME
#     content got its old ID back in one probe, so IDs are not strictly "never reused".
#
#   pre:  gap-1-value       unique schema, then hard-deleted          -> ID gap
#         similar-ns-value  same fields as Order, other namespace     -> different ID
#         similar-doc-value same as Order + a "doc" attribute         -> different ID
#         gap-2-value       unique schema, then hard-deleted          -> ID gap
#   mid:  orders-copy-value byte-identical copy of orders-value v1    -> SAME ID as orders v1
#         gap-3-value       unique schema, then hard-deleted          -> ID gap
#         lookalike-v2-value Order v2 with a different default         -> different ID
#
# Requires deletion to be enabled on Apicurio 3.x (see docker-compose.yml).
set -euo pipefail

PHASE="${1:?usage: $0 pre|mid}"
CC="${APICURIO_CC:-http://localhost:8081/apis/ccompat/v7}"
V2="${APICURIO_V2:-http://localhost:8081/apis/registry/v2}"

register() {   # subject, schema-json  -> prints id
  local body
  body=$(python3 -c 'import json,sys; print(json.dumps({"schema": sys.argv[1]}))' "$2")
  curl -sf -X POST "$CC/subjects/$1/versions" \
    -H 'Content-Type: application/vnd.schemaregistry.v1+json' -d "$body" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])'
}

hard_delete() {  # subject
  curl -sf -X DELETE "$CC/subjects/$1" >/dev/null
  curl -sf -X DELETE "$CC/subjects/$1?permanent=true" >/dev/null
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' "$CC/subjects/$1/versions")
  if [[ "$code" != "404" ]]; then
    echo "ERROR: $1 still present after hard delete (HTTP $code). Is deletion enabled?" >&2
    exit 1
  fi
}

ns='"namespace":"io.ibm.migration"'
order_fields='[{"name":"orderId","type":"string"},{"name":"customer","type":"string"},{"name":"amount","type":"double"}]'

case "$PHASE" in
  diverge)
    # Register a unique schema under a throwaway subject to advance the globalId counter
    # past the next contentId slot. When the next UNIQUE content is registered (e.g.
    # orders-value), its globalId will be higher than its contentId, producing the
    # divergence the Part B test cases require.
    # Counter state example after "pre" (4 ops, 2 hard-deleted):
    #   globalId/contentId counter is at 5. Register a throwaway unique schema -> gid=5/cid=5.
    #   Hard-delete it -> gap at 5. Now register orders-value -> gid=6/cid=6? No — hard deletes
    #   leave the counter advanced but the content hash is gone. Re-registering unique content
    #   gets a new slot.
    # Simpler: just register a schema with unique content (not hard-deleted) so the counter
    # advances. The NEXT unique registration (orders-value via the producer) gets a
    # globalId one higher, but contentId may still be the next available slot too.
    # The actual divergence is that orders-copy-value (mid phase) shares contentId with
    # orders-value but has a different globalId; use orders-copy-value as the test subject
    # for cases 3/4 instead.
    #
    # This phase is a no-op placeholder — the real divergence for Part B cases 3 and 4
    # is produced by running the full edge-case demo (pre+produce+mid) then checking
    # orders-copy-value: it has globalId=N but contentId=orders-value-v1-id.
    echo "diverge: orders-copy-value will have globalId != contentId after the mid phase."
    echo "Use SUBJECTS=orders-copy-value with migrate-schema.sh for cases 3 and 4."
    ;;
  pre)
    id=$(register gap-1-value '{"type":"record","name":"Gap1","fields":[{"name":"g","type":"string"}]}')
    hard_delete gap-1-value
    echo "gap-1-value        id=$id  (hard-deleted -> gap)"

    id=$(register similar-ns-value "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"io.ibm.other\",\"fields\":$order_fields}")
    echo "similar-ns-value   id=$id  (Order fields, other namespace)"

    id=$(register similar-doc-value "{\"type\":\"record\",\"name\":\"Order\",$ns,\"doc\":\"lookalike\",\"fields\":$order_fields}")
    echo "similar-doc-value  id=$id  (Order + doc attribute)"

    id=$(register gap-2-value '{"type":"record","name":"Gap2","fields":[{"name":"g","type":"long"}]}')
    hard_delete gap-2-value
    echo "gap-2-value        id=$id  (hard-deleted -> gap)"
    ;;
  mid)
    # Exact content of orders-value v1 as the serializer registered it.
    v1=$(curl -sf "$CC/subjects/orders-value/versions/1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["schema"])')
    id=$(register orders-copy-value "$v1")
    v1id=$(curl -sf "$CC/subjects/orders-value/versions/1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
    echo "orders-copy-value  id=$id  (identical to orders-value v1, id=$v1id)"
    if [[ "$id" != "$v1id" ]]; then
      echo "NOTE: identical content did not share an ID (canonical hashing/dedup differs)." >&2
    fi

    id=$(register gap-3-value '{"type":"record","name":"Gap3","fields":[{"name":"g","type":"int"}]}')
    hard_delete gap-3-value
    echo "gap-3-value        id=$id  (hard-deleted -> gap)"

    id=$(register lookalike-v2-value "{\"type\":\"record\",\"name\":\"Order\",$ns,\"fields\":[{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"customer\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\",\"default\":\"GBP\"}]}")
    echo "lookalike-v2-value id=$id  (Order v2 shape, different default)"
    ;;
  *)
    echo "usage: $0 pre|mid" >&2; exit 2 ;;
esac

echo
echo "Apicurio IDs now (subject version -> ccompat id / globalId / contentId):"
for s in $(curl -sf "$CC/subjects" | python3 -c 'import json,sys; print(" ".join(sorted(json.load(sys.stdin))))'); do
  curl -sf "$V2/groups/default/artifacts/$s/versions" | python3 <(cat <<'PY'
import json, sys
subject = sys.argv[1]
for v in json.load(sys.stdin).get("versions", []):
    print("  %-20s v%-3s globalId=%-4s contentId=%s" % (subject, v["version"], v["globalId"], v.get("contentId")))
PY
) "$s"
done
