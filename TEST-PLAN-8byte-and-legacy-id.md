# Test Plan: 8-byte Apicurio IDs and Legacy ID Mode

## Status

| Section | Status |
| --- | --- |
| Part A — Edge-case ID verification | ✅ All phases complete (A1–A5) |
| Part B — Phase 0 code changes | ✅ Built and compiled |
| Part B — Phase 0 exit check (legacy mode) | ✅ Confirmed — `orders-copy-value` ccompat `.id` = globalId=6 with `LEGACY_ID_MODE=true` |
| Part B — Phase 0b (2.x → 3.3.2 upgrade path) | ✅ Complete for cases 1, 2, 6 — IDs preserved; case 6 gap confirmed through upgrade |
| Part B — Test matrix cases 1–2 | ✅ PASS (confirmed) |
| Part B — Test matrix cases 3–4 | ✅ PASS (expected FAIL, got FAIL — gap confirmed) |
| Part B — Test matrix cases 5–8 | ✅ Complete — 5 PASS, 6 FAIL (expected) |

---

## Part A — Edge-case ID verification ✅ COMPLETE

### A.0 What was built and why

Goal: prove that **the schema ID inside every Confluent-side payload is the Apicurio ID**, even when Apicurio
IDs are **not contiguous** (deletes, lookalike schemas, shared content), and that Confluent IMPORT mode
**keeps the exact ID** that you give it.

| Piece | File | What it does |
| --- | --- | --- |
| Deletes enabled | `docker-compose.yml` (`APICURIO_REST_DELETION_*`) | 3.3.2 disables deletes by default (it returned HTTP 405). Also, `APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED` is now `${LEGACY_ID_MODE:-false}` |
| Edge-case seeding | `scripts/seed-edge-case-schemas.sh pre\|mid` | `pre`: 2 schemas registered then **hard-deleted** (gaps), plus 2 lookalikes of `Order` (other namespace; added `doc`). `mid`: a byte-identical copy of `orders-value` v1 under another subject (shares its contentId), 1 more hard-deleted schema, plus a v2 lookalike with a different default |
| Second schema on the topic | `produce-v2 [n]` CLI command (`OrderSchema.orderV2`) | Evolves `orders-value` (adds `currency` with a default), so the topic carries **two** schema IDs |
| Migrate everything | `scripts/migrate-schema.sh` (rewritten) | All subjects and all versions, IMPORT with explicit `id` + `version`. **Fails** if the Confluent-returned ID ≠ the Apicurio ccompat ID, or if the schema behind the ID differs. `SUBJECTS="..."` limits it to the listed subjects |
| Payload ID check | `scripts/verify-payload-ids.sh` → `verify-ids` (`apps/.../verify/PayloadIdVerifier.java`) | For each dest record, matched to the source by offset: key equal; byte 0 = `0x00`; bytes 1–4 = the Apicurio header ID (it honours `PREFER_CONTENT_ID`); Avro body unchanged; the ID resolves in Confluent, and Confluent's schema == Apicurio ccompat's schema for that ID (this catches a "valid ID, wrong schema"). Prints a per-record table plus `id -> record count` |
| IMPORT-mode negatives | `scripts/verify-import-mode.sh` | (1) same schema + same ID under a new subject → accepted; (2) **different** schema + a taken ID → rejected `42205`; (3) same schema + a new unused ID → accepted, and the original ID still resolves. Cleans up its throwaway subjects |
| Wiring | `scripts/run-demo.sh` | `EDGE_CASES=1` adds steps 4b, 5b and 9d. Step 9c (the payload ID check) now **always** runs |

### A.1 Verified findings

- **globalId and contentId share one counter in Apicurio 3.3.2.**
  After the `pre` seeding (4 registrations, 2 hard-deleted), the next available slot is 5.
  `orders-value` v1 gets globalId=5/contentId=5. After `mid`, `orders-copy-value` (identical content)
  gets globalId=6 but reuses contentId=5. Any subsequent unique schema gets a globalId that is
  higher than its contentId — **this is the divergence**.
- **Hard deletes create permanent gaps.** IDs 1 and 4 were deleted; `/schemas/ids/1` and `/schemas/ids/4`
  return 404 and are not reassigned.
- **Identical content shares a contentId across subjects.** `orders-value` v1 and `orders-copy-value`
  both resolve to contentId=5. Confluent correctly imports this as a single schema id=5 shared by two subjects.
- **The 2.6.5 serde with `USE_ID=contentId` writes only `apicurio.value.contentId` in the header.**
  The `apicurio.value.globalId` header is absent. With `USE_ID=globalId` only `apicurio.value.globalId`
  is written. Both are 8-byte big-endian longs.

### A.2 Phase results

**A1 — Default mode regression:** ✅ PASS (`exit=0`)

```
subject              ver  apicurio   confluent    result
orders-value         v1   1          1            OK

offset key        expected-id    payload-id   body     schema check
0      key-1      1              1            same     OK schema matches [orders-value v1]
...
Schema IDs seen on Confluent payloads (id -> records): {1=5}
Payload ID verification PASSED
SUCCESS: Confluent consumer read 5 rewritten record(s) without Apicurio.
```

**A2 — Edge-case run:** ✅ PASS (`EDGE_CASES=1`, `exit=0`)

Seed output confirming gaps and content sharing:
```
gap-1-value        id=1  (hard-deleted -> gap)
similar-ns-value   id=2  (Order fields, other namespace)
similar-doc-value  id=3  (Order + doc attribute)
gap-2-value        id=4  (hard-deleted -> gap)
orders-value v1 produced with contentId=5

orders-copy-value  id=5  (identical to orders-value v1, id=5)   ← shared contentId
gap-3-value        id=7  (hard-deleted -> gap)
lookalike-v2-value id=8  (Order v2 shape, different default)
orders-value v2 produced with contentId=9
```

Final ID table:
```
  lookalike-v2-value   v1   globalId=8    contentId=8
  orders-copy-value    v1   globalId=6    contentId=5   ← diverges
  orders-value         v1   globalId=5    contentId=5
  similar-doc-value    v1   globalId=3    contentId=3
  similar-ns-value     v1   globalId=2    contentId=2
```

**A3 — Evidence checked:**

| Check | Expected | Actual | Result |
| --- | --- | --- | --- |
| IDs have gaps | orders-value v1/v2 IDs not 1 and 2 | v1=5, v2=9; IDs 1,4,7 absent | ✅ |
| Identical content shares an ID | orders-copy-value id == orders-value v1 id | Both contentId=5 | ✅ |
| Lookalikes get their own IDs | similar-ns=2, similar-doc=3, lookalike-v2=8 | All distinct from 5 and 9 | ✅ |
| Migration kept every ID | all rows OK | 6 rows all OK, including shared id=5 across 2 subjects | ✅ |
| Each payload carries the right ID | 8 rows, payload-id==header-id, body same | `{5=5, 9=3}` — all same, schema matches | ✅ |
| Confluent rejects collisions | check 2 shows 42205 | `{"error_code":42205,"message":"Overwrite new schema with id 9 is not permitted."}` | ✅ |
| Consumers decode | SUCCESS ... 8 records, v2 shows currency: EUR | Exact match | ✅ |

**A4 — Independent cross-check (raw bytes + curl):** ✅

```
00000000: 0000 0000 050a 4f52 442d 31...   ← magic + id=5 + Avro body (v1 records)
00000090: ...0000 0000 090f...              ← magic + id=9 (v2 records)

Confluent v1 id=5, v2 id=9
Apicurio ccompat v2 id=9  ← matches Confluent
```

**A5 — Negative control:** ✅ Verifier correctly exits 1

After hard-deleting id=9 from Confluent only:
```
5      v2-key-1   9              9            same     FAIL: Confluent /schemas/ids/9 -> HTTP 404
6      v2-key-2   9              9            same     FAIL: Confluent /schemas/ids/9 -> HTTP 404
7      v2-key-3   9              9            same     FAIL: Confluent /schemas/ids/9 -> HTTP 404
Payload ID verification FAILED: 3 problem(s).
exit=1
```
The 5 v1 rows are unaffected. The check is able to fail and reports exactly the right rows.

### A.3 Known limits of Part A

- Only **header-mode** producers, and only **contentId** (`USE_ID=contentId`, legacy flag `false`). Legacy mode and 8-byte payload framing are Part B.
- The source registry is fresh 3.3.2, not data upgraded from 2.x (that's Part B, Phase 0b).
- Single partition; MirrorSourceConnector, not Confluent Replicator.

---

## Part B — 8-byte IDs and legacy ID mode

### Handoff prompt (paste into a new window)

> Work in `05-header-wire-format-poc/header-replciator-poc`. Read this file first (especially the
> "ID divergence blocker" section and the updated Phase 0 table), then continue from where work stopped.
> Cases 1 and 2 are confirmed PASS. The blocker for cases 3 and 4 is that `orders-value` v1 always
> lands with globalId=contentId in a clean registry. Fix: add `APICURIO_SUBJECT` env support to
> `ApicurioHeaderProducer` so cases 3/4 can produce via the `orders-copy-value` subject (which has
> globalId=6 ≠ contentId=5 after the mid-phase seeding). Then run cases 3–8 and Phase 0b.
> Don't mark anything as passing without evidence.

### Event Streams context (why this matters)

- Event Streams started on Apicurio 2.x and has moved to 3.x. Because 3.x has breaking API changes, ES
  keeps serving the **`/apis/registry/v2`** endpoint, and clients keep using it. Customers will migrate to
  Confluent + Confluent Schema Registry **from Apicurio 3.3.x**, still through `/v2` (and ccompat for the export).
- Kafka records are immutable. Every ID in a record written in the 2.x era is still that ID today. Only
  Apicurio's storage changed, during its own 2.x → 3.x data migration.
- So the real questions are:
  1. After the 2.x → 3.x upgrade, does each **old** in-record ID (globalId or contentId) still resolve to the
     **same schema** in 3.3.x, both via `/v2` and via ccompat? This is assumed today, not proven.
  2. Does the ccompat export from 3.3.x give Confluent the IDs those old records carry, with the legacy flag
     set the way that customer's ES had it?
  3. Can the SMT rewrite **every framing an ES client may have used**? A client's framing depends on its
     **serde version and config**, not on the server version. ES clients on the `/v2` endpoint are likely on 2.x
     serdes, so 2.x-default framing (`00` + 8-byte payload ID when headers are off) is a **main path**, not an
     edge case.

### Background (verified so far)

- Header mode (`ENABLE_HEADERS=true`) writes `apicurio.value.contentId` or `apicurio.value.globalId` as
  **8-byte big-endian longs** — confirmed on 2.6.5 serde. **Only the header matching `USE_ID` is written**
  (not both). On the topic it shows up as `00 00 00 00 00 00 00 05`.
- Payload mode (headers off):
  - 2.x default is `DefaultIdHandler`: `00` + **8-byte** ID. `Legacy4ByteIdHandler` is opt-in.
  - 3.x default is `Default4ByteIdHandler`: `00` + **4-byte** ID. `Legacy8ByteIdHandler` is opt-in.
- The SMT reads the 8-byte (or 4-byte) header, narrows it to an int and prepends `00` + 4-byte ID. It does **not** call any registry.
- `migrate-schema.sh` imports each version into Confluent using the `id` returned by **ccompat**. Which ID that is depends on legacy ID mode:

| `APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED` | ccompat `id` is | Producer must write | SMT setting |
| --- | --- | --- | --- |
| `false` | contentId | `USE_ID=contentId` | `prefer.content.id=true` |
| `true` | globalId | `USE_ID=globalId` | `prefer.content.id=false` |

### Unverified assumptions — status

| # | Assumption | Status |
| --- | --- | --- |
| 1 | Apicurio 3.3.2 supports legacy ID mode; `APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED` is the right env var | ✅ **Confirmed**: Phase 0 exit check ran with `LEGACY_ID_MODE=true` on `orders-copy-value` (globalId=6 ≠ contentId=5). ccompat returned `id=6` (globalId). `/schemas/ids/6` returns HTTP 200. |
| 2 | 2.6.5 serializer writes only the `USE_ID` header, not both | ✅ **Confirmed**: `USE_ID=contentId` → only `apicurio.value.contentId` header present; `USE_ID=globalId` → only `apicurio.value.globalId` header present |
| 3 | Default `USE_ID` for the 2.6.5 serde | ✅ **Confirmed**: default is `contentId` (verified in A1/A2 — header `apicurio.value.contentId` present with no explicit config) |
| 4 | ccompat `/schemas/ids/{id}` lookup also follows the legacy flag | ✅ **Confirmed**: Phase 0 exit check verified `/schemas/ids/6` and `/schemas/ids/5` both resolve with `LEGACY_ID_MODE=true`. ccompat id=6 (globalId) returned for the diverged subject. |
| 5 | 2.x export imported into 3.3.2 keeps every globalId and contentId unchanged | ✅ **Confirmed**: Phase 0b cases 1, 2, 6. Apicurio 2.6.13 export (`/v2/admin/export`) imported into 3.3.2 via `/v3/admin/import` (HTTP 204). `ids-before.txt` == `ids-after.txt` in all three cases: `orders-value v1 globalId=5 contentId=5` unchanged. |

### Phase 0 — Code changes

**Built and compiled (all changes are in the working tree):**

| File | Change | Status |
| --- | --- | --- |
| `apps/.../ApicurioHeaderProducer.java` | `APICURIO_ENABLE_HEADERS`, `APICURIO_USE_ID`, `APICURIO_ID_HANDLER` env vars; prints all `apicurio.*` headers with byte lengths | ✅ Done |
| `scripts/start-connector.sh` | `prefer.content.id` = `${PREFER_CONTENT_ID:-true}` | ✅ Done |
| `apps/.../PayloadIdVerifier.java` | `EXPECTED_ID_SOURCE=headers\|payload8\|payload4`; payload-mode strips framing prefix before body comparison | ✅ Done |
| `scripts/show-ids.sh` (new) | Prints globalId, contentId, ccompat id, Confluent id, raw header bytes side by side | ✅ Done |
| `scripts/run-case.sh` (new) | Runs one matrix case end-to-end from a clean stack, prints PASS/FAIL | ✅ Done |
| `apps/.../ApicurioHeaderProducer.java` | `APICURIO_SUBJECT` env var — overrides `EXPLICIT_ARTIFACT_ID` so the serde resolves under an explicit subject instead of the topic name | ✅ Done |
| `scripts/run-case.sh` | Cases 3/4 updated: produce 1 seed record under default subject, seed mid, then produce 5 records under `APICURIO_SUBJECT=orders-copy-value`; `MIGRATE_SUBJECT=orders-copy-value` | ✅ Done |
| `scripts/run-phase0b.sh` (new) | Phase 0b driver — starts Apicurio 2.6.13, seeds, produces, exports, swaps to 3.3.2, imports, diffs IDs, verifies all records | ✅ Done |

**Phase 0 exit check:** ✅ Confirmed. `LEGACY_ID_MODE=true`, after pre+produce+mid seeding:
- `orders-copy-value v1`: globalId=6, contentId=5
- ccompat `.id` = **6** (globalId) ✅
- `/schemas/ids/6` → HTTP 200, `/schemas/ids/5` → HTTP 200 ✅

### ID divergence — resolved

`orders-value` v1 always lands with **globalId = contentId** in a clean registry. After the mid-phase,
`orders-copy-value` (byte-identical content) gets a new globalId but reuses orders-value's contentId:

```
orders-value  v1:  globalId=5  contentId=5
orders-copy   v1:  globalId=6  contentId=5   ← diverges
```

`APICURIO_SUBJECT=orders-copy-value` was added to `ApicurioHeaderProducer` (uses `SerdeConfig.EXPLICIT_ARTIFACT_ID`
to override the TopicIdStrategy result). Cases 3 and 4 now produce via `orders-copy-value`, migrate that
subject (not `orders-value`), and confirm the mismatch.

**What cases 3 and 4 proved:**
- Case 3 (`LEGACY_ID_MODE=true`, `USE_ID=contentId`, `PREFER_CONTENT_ID=true`):
  Producer header = contentId=5. Confluent imported id=6 (globalId, because `LEGACY_ID_MODE=true`).
  SMT wrote payload-id=5. Confluent `/schemas/ids/5` → HTTP 404. All 6 records FAIL. ✅
- Case 4 (`LEGACY_ID_MODE=false`, `USE_ID=globalId`, `PREFER_CONTENT_ID=false`):
  Producer header = globalId=6. Confluent imported id=5 (contentId, because `LEGACY_ID_MODE=false`).
  SMT wrote payload-id=6. Confluent `/schemas/ids/6` → HTTP 404 (5 records). The seed record (offset 0,
  produced under `orders-value` which has globalId=contentId=5) still resolves as id=5 → that one passes.
  Net: 5 failures. ✅

### Phase 0b — Reproduce the ES upgrade path (2.x data → 3.3.2) ✅ Complete

Run via `bash scripts/run-phase0b.sh <1|2|6>`. The script starts Apicurio 2.6.13, seeds, produces "old"
records, saves `ids-before.txt`, exports via `/v2/admin/export`, swaps to 3.3.2, imports via
`/v3/admin/import` (HTTP 204 confirmed), saves `ids-after.txt`, diffs them (must be empty), produces
new records, migrates to Confluent, and verifies all records. Results are in the Phase 0b table in
the Final Deliverables section below.

**Key finding:** `/v3/admin/import` (HTTP 204) is the working endpoint for a 2.x zip. The `/v2` endpoint
is not needed. IDs are preserved exactly — the `ids-before` and `ids-after` files are identical for
all three cases tested.

### Phase 1 — Test matrix

Run every case from a clean stack (`docker-compose down -v`). Use `scripts/run-case.sh <n>`.

| # | Legacy mode | Producer framing | `USE_ID` | `PREFER_CONTENT_ID` | Expected | Result | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | false | headers (8-byte) | contentId | true | **PASS** | ✅ PASS | orders-value v1 globalId=contentId=5; both IDs equal, SMT correct |
| 2 | true | headers (8-byte) | globalId | false | **PASS** | ✅ PASS | orders-value v1 globalId=contentId=5; ccompat returns globalId=5; aligned |
| 3 | true | headers (8-byte) | contentId | true | **FAIL** | ✅ FAIL | orders-copy-value: header=contentId=5, Confluent id=globalId=6. Confluent `/schemas/ids/5` → 404. All 6 records fail. consume-cflt: `Schema 5 not found; error code: 40403` |
| 4 | false | headers (8-byte) | globalId | false | **FAIL** | ✅ FAIL | orders-copy-value: header=globalId=6, Confluent id=contentId=5. Confluent `/schemas/ids/6` → 404 (5 records). 1 seed record (id=5) passes. Net: 5 failures. consume-cflt fails at offset 1. |
| 5 | true | headers (8-byte) | globalId | **true** | PASS | ✅ PASS | orders-value: globalId=contentId=5; only `apicurio.value.globalId` header present; `PREFER_CONTENT_ID=true` falls back to globalId=5. SMT writes id=5. Confluent resolves OK. |
| 6 | false | payload 2.x 8-byte | contentId | n/a | **FAIL** (known gap) | ✅ FAIL | SMT reads bytes 1-4 of the 8-byte ID (`00 00 00 00`) → payload-id=0. Confluent has id=5. All 5 records fail with payload ID 0 ≠ expected 5. |
| 7 | false | payload 4-byte (`Legacy4ByteIdHandler`) | contentId | n/a | **PASS** | ✅ PASS | Source `00 + 4-byte id=5` passes through unchanged. Confluent reads id=5 correctly. All 5 records pass. |
| 8 | true | payload 4-byte | globalId | n/a | **PASS** | ✅ PASS | Same as case 7 in legacy mode. globalId=contentId=5 for `orders-value`; no divergence. All 5 records pass. |

Optional case **3b**: in case 3, also import the decoy subjects into Confluent so the wrong ID *does* exist there. Check whether the consumer decodes using the wrong schema (garbage or exception) instead of getting a 404. Document if it happens — this is the most dangerous failure mode.

### Phase 2 — Pass/fail criteria for each case

Record all of these:

1. The `globalId`, `contentId`, ccompat `id` and Confluent `id` for the test subject (from `show-ids.sh`).
2. The headers on the source topic, with names and byte lengths (from producer output).
3. The output of `verify-payload-ids.sh` (the per-record table). Every payload ID must equal the expected ID, and the schema check must say `OK`.
4. The output of `consume-cflt`: a PASS means 5 records whose values match what was produced. Note any exception text.
5. Connector task state (`/connectors/es-to-cflt-header-rewrite/status`). If the SMT throws, the task shows FAILED.

A case only passes if its observed result matches the **Expected** column. An "unexpected PASS" (a case expected to fail that passes) is a finding too — it means the IDs didn't actually diverge; check `show-ids.sh` output.

### Phase 3 — Only if case 6 fails as expected

Decide whether to extend the SMT with payload-mode support. One option: a `payload.id.length` setting (`4` or `8`). When it is `8` and `payload[0]==0x00`, rewrite `00` + 8-byte to `00` + 4-byte. Note that the current check ("byte 0 is `0x00` so it's already Confluent wire format") cannot distinguish 4-byte from 8-byte framing. Any fix must be driven by config, not guessed from the bytes. Add a case 6b to verify it.

### ✅ All action items complete

All items from the "Next session" list are done.

---

## Final Deliverables

### Results table (all 8 cases)

| # | Legacy | Framing | `USE_ID` | `PREFER_CONTENT_ID` | Expected | Result | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | false | headers 8B | contentId | true | PASS | ✅ PASS | `{5=5}` all OK; consume-cflt 5 records |
| 2 | true | headers 8B | globalId | false | PASS | ✅ PASS | `{5=5}` all OK; consume-cflt 5 records |
| 3 | true | headers 8B | contentId | true | FAIL | ✅ FAIL | `{5=6}` — header contentId=5, Confluent id=6 (globalId). `/ids/5` → 404. `Schema 5 not found` |
| 4 | false | headers 8B | globalId | false | FAIL | ✅ FAIL | `{5=1, 6=5}` — header globalId=6, Confluent id=5 (contentId). `/ids/6` → 404. 5/6 records fail |
| 5 | true | headers 8B | globalId | true | PASS | ✅ PASS | `{5=5}` — no contentId header; SMT falls back to globalId; both equal 5 |
| 6 | false | payload 8B (`DefaultIdHandler`) | contentId | n/a | FAIL | ✅ FAIL | bytes 1-4 of 8-byte ID = `00 00 00 00` → payload-id=0. `/ids/0` → 404 |
| 7 | false | payload 4B (`Legacy4ByteIdHandler`) | contentId | n/a | PASS | ✅ PASS | 4-byte `00+id=5` identical to Confluent wire; all OK |
| 8 | true | payload 4B (`Legacy4ByteIdHandler`) | globalId | n/a | PASS | ✅ PASS | same as case 7 in legacy mode; all OK |

### Verified answers to the five assumptions

| # | Assumption | Answer |
| --- | --- | --- |
| 1 | `APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED` enables legacy ID mode in 3.3.2 | ✅ **Confirmed.** env var wired in `docker-compose.yml`; Phase 0 exit check with a diverged subject (orders-copy-value globalId=6 ≠ contentId=5) returned ccompat `id=6` (globalId) with `LEGACY_ID_MODE=true`. |
| 2 | 2.6.5 serde writes only the `USE_ID` header | ✅ **Confirmed.** `USE_ID=contentId` → only `apicurio.value.contentId`; `USE_ID=globalId` → only `apicurio.value.globalId`. Never both. |
| 3 | Default `USE_ID` for the 2.6.5 serde is `contentId` | ✅ **Confirmed.** Default produce (no explicit `USE_ID`) always writes `apicurio.value.contentId`. |
| 4 | ccompat `/schemas/ids/{id}` follows the legacy flag | ✅ **Confirmed.** With `LEGACY_ID_MODE=true`: `/schemas/ids/6` → HTTP 200 (globalId); `/schemas/ids/5` → HTTP 200 (contentId, shared). With `LEGACY_ID_MODE=false`: `/schemas/ids/5` → HTTP 200 (contentId). |
| 5 | 2.x export imported into 3.3.2 preserves globalId and contentId | ✅ **Confirmed.** Phase 0b cases 1, 2, 6: `apicurio/apicurio-registry-mem:2.6.13.Final` exported via `/v2/admin/export`; imported into 3.3.2 via `/v3/admin/import` (HTTP 204). `ids-before.txt` == `ids-after.txt` in all three cases. `orders-value v1 globalId=5 contentId=5` preserved exactly. |

### Phase 0b: 2.x → 3.3.2 upgrade path

| Case | IDs preserved | Old records decode (3.3.2) | verify-payload-ids | consume-cflt | Result |
| --- | --- | --- | --- | --- | --- |
| 1 | ✅ yes (gid=cid=5 before and after) | ✅ 5 records | ✅ PASS (8 records, old+new) | ✅ 8 records | ✅ PASS |
| 2 | ✅ yes | ✅ 5 records | ✅ PASS (8 records) | ✅ 8 records | ✅ PASS |
| 6 | ✅ yes | ✅ 5 records | ❌ FAIL (payload-id=0) | ❌ fail | ❌ FAIL (same gap as case 6; IDs survived but SMT cannot rewrite 8-byte payloads) |

### Recommendations per producer configuration

| Client config | `LEGACY_ID_MODE` on ES | SMT `prefer.content.id` | Safe to migrate? | Notes |
| --- | --- | --- | --- | --- |
| `ENABLE_HEADERS=true`, `USE_ID=contentId` | false | `true` | ✅ Yes | Default ES header config. Cases 1 and Phase 0b case 1 both pass. |
| `ENABLE_HEADERS=true`, `USE_ID=globalId` | true | `false` | ✅ Yes | Cases 2 and Phase 0b case 2 both pass. |
| `ENABLE_HEADERS=true`, `USE_ID=contentId` | **true** | any | ❌ No — fix needed | Case 3: contentId in header; Confluent only has globalId. Producer and registry must use **matching** ID type. Either switch `USE_ID=globalId` or keep `LEGACY_ID_MODE=false`. |
| `ENABLE_HEADERS=true`, `USE_ID=globalId` | **false** | any | ❌ No — fix needed | Case 4: globalId in header; Confluent only has contentId. Matching rule applies in reverse. |
| `ENABLE_HEADERS=true`, `USE_ID=globalId` | true | `true` (mismatch) | ✅ Yes (SMT fallback) | Case 5: SMT falls back from absent contentId header to globalId. Works when globalId=contentId (as here). Verify before relying on this for diverged subjects. |
| `ENABLE_HEADERS=false`, default handler (8-byte) | false | n/a | ❌ No — Phase 3 fix required | Case 6: SMT misreads 8-byte Apicurio payload as 4-byte Confluent wire. payload-id always 0. Requires `payload.id.length=8` SMT config (Phase 3). |
| `ENABLE_HEADERS=false`, `Legacy4ByteIdHandler` | false | n/a | ✅ Yes | Case 7: 4-byte payload already matches Confluent wire; pass-through correct. |
| `ENABLE_HEADERS=false`, `Legacy4ByteIdHandler` | true | n/a | ✅ Yes | Case 8: same as case 7 in legacy mode. |

### What to collect from each customer before migrating

1. **Serde version** per client application (2.x or 3.x).
2. **`ENABLE_HEADERS`** setting (or serde property `apicurio.registry.avro.encoding`). If headers are disabled, ask for the id-handler class.
3. **`USE_ID`** setting (`contentId` or `globalId`; default is `contentId` on 2.x).
4. **`APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED`** (server-side) — the ES admin must confirm this. It controls whether ccompat exports globalId or contentId as the schema `id`.
5. **Id-handler class** when headers are off — `DefaultIdHandler` (2.x default, 8-byte) vs `Legacy4ByteIdHandler` (4-byte opt-in). If using the 2.x default payload mode, Phase 3 SMT extension is required before migration.
6. Confirm the customer has **not** mixed `USE_ID` settings across producer versions writing to the same topic — if they have, per-record header inspection is needed.
