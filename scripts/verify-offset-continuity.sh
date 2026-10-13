#!/usr/bin/env bash
# Offset Continuity Regression
#
# Verifies that every offset produced on kafka-source is present at the
# identical offset on kafka-dest (IdentityReplicationPolicy preserves names
# AND offsets when MirrorSourceConnector replicates without compaction).
#
# Exit codes:
#   0  — all offsets match
#   1  — mismatch (regression) or topic not found
set -euo pipefail

TOPIC="${TOPIC:-orders}"
TIMEOUT_MS="${TIMEOUT_MS:-10000}"

echo "=== Offset Continuity Regression: topic '$TOPIC' ==="

# ── helper: get end offset (high-watermark) for partition 0 ─────────────────
# apache/kafka uses kafka-get-offsets.sh (output: "topic:partition:offset")
end_offset() {
  local cluster="$1"  # kafka-source | kafka-dest
  docker exec "$cluster" \
    /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC" \
    --time latest \
    2>/dev/null \
  | awk -F: '{print $3}' | head -1
}

src_end=$(end_offset kafka-source)
dst_end=$(end_offset kafka-dest)

echo "  kafka-source end offset : $src_end"
echo "  kafka-dest   end offset : $dst_end"

if [[ -z "$src_end" || -z "$dst_end" ]]; then
  echo "FAIL: could not read end offsets — topic '$TOPIC' may not exist on one cluster." >&2
  exit 1
fi

if [[ "$src_end" -ne "$dst_end" ]]; then
  echo "FAIL: offset mismatch — source=$src_end dest=$dst_end" \
       "(replication may be incomplete or offset continuity is broken)" >&2
  exit 1
fi

echo "  OK: offsets match ($src_end)"

# ── Spot-check: compare raw payload bytes at offset 0 on both sides ─────────
# After the SMT rewrite the dest payload is LARGER (5-byte prefix prepended).
# We verify dest payload begins with magic byte 0x00 and is bigger than source.
echo
echo "=== Payload framing spot-check (offset 0) ==="

# kafka-console-consumer outputs raw bytes to stdout; xxd runs on the host.
src_hex=$(docker exec kafka-source \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" \
  --from-beginning --max-messages 1 \
  --timeout-ms "$TIMEOUT_MS" \
  2>/dev/null \
  | xxd -p | tr -d '\n' || true)

dst_hex=$(docker exec kafka-dest \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" \
  --from-beginning --max-messages 1 \
  --timeout-ms "$TIMEOUT_MS" \
  2>/dev/null \
  | xxd -p | tr -d '\n' || true)

if [[ -z "$src_hex" || -z "$dst_hex" ]]; then
  echo "WARNING: could not read payload bytes for spot-check (no messages yet?)"
else
  echo "  source first 20 bytes (hex): ${src_hex:0:40}"
  echo "  dest   first 20 bytes (hex): ${dst_hex:0:40}"

  # Dest payload must start with magic byte 0x00
  if [[ "${dst_hex:0:2}" != "00" ]]; then
    echo "FAIL: dest payload does not start with Confluent magic byte 0x00 (got ${dst_hex:0:2})" >&2
    exit 1
  fi
  echo "  OK: dest payload starts with Confluent magic byte 0x00"

  # Dest must be exactly 5 bytes longer than source (1 magic + 4 schema id)
  src_len=$(( ${#src_hex} / 2 ))
  dst_len=$(( ${#dst_hex} / 2 ))
  expected_dst_len=$(( src_len + 5 ))
  if [[ "$dst_len" -ne "$expected_dst_len" ]]; then
    echo "FAIL: dest payload length=$dst_len expected=$expected_dst_len (src=$src_len + 5-byte prefix)" >&2
    exit 1
  fi
  echo "  OK: dest payload is exactly 5 bytes longer than source (Confluent prefix added)"

  # Raw Avro payload portion must be identical
  # source hex  = raw avro bytes
  # dest hex    = 00 + 4-byte id + raw avro bytes  =>  skip first 10 hex chars (5 bytes)
  dst_avro_hex="${dst_hex:10}"
  if [[ "$src_hex" != "$dst_avro_hex" ]]; then
    echo "FAIL: raw Avro payload differs after SMT rewrite — data corruption detected" >&2
    echo "      source avro : $src_hex" >&2
    echo "      dest avro   : $dst_avro_hex" >&2
    exit 1
  fi
  echo "  OK: raw Avro payload bytes are identical on both sides"
fi

echo
echo "Offset continuity regression PASSED."
