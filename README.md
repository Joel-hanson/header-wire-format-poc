# Header wire format → Confluent (SMT PoC)

Apicurio header-mode Avro does not look like Confluent Avro on the wire. This PoC rewrites records **in flight** with a Connect SMT so destination consumers use stock Confluent `KafkaAvroDeserializer` — no Apicurio after cutover.

## The problem

```text
Source (Apicurio headers)              Dest (Confluent expects)
─────────────────────────              ───────────────────────
headers: apicurio.value.globalId       payload: 0x00 + 4-byte id + Avro
payload: raw Avro only
```

Same Avro body. Different framing. A Confluent consumer will reject the source bytes as-is.

## The idea

```text
Producer (Apicurio headers)
        │
        ▼
  kafka-source  ──MirrorSource + SMT──►  kafka-dest
                                              │
                                              ▼
                         Consumer (Confluent KafkaAvroDeserializer)
                         + Schema Registry (IDs imported from Apicurio)
```

The SMT reads the 8-byte ID from the header, prepends `0x00` + 4-byte ID, and strips Apicurio headers. Schemas must already exist in Confluent under the **same numeric IDs** (ccompat export → IMPORT).

> Confluent Replicator is license-gated. This stack uses MirrorSourceConnector with the **same SMT**. See `connectors/replicator-example.json` for the Replicator shape.

## Defaults this demo uses

Matches typical Event Streams / Apicurio **2.x** clients:

| Setting | Value |
|---|---|
| `ENABLE_HEADERS` | `true` |
| `USE_ID` | `globalId` |
| SMT `prefer.content.id` | `false` (read globalId header) |
| `APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED` | `true` (ccompat `.id` = globalId) |

Producer header ID, ccompat export ID, and SMT rewrite ID must be the **same number**. Override with `LEGACY_ID_MODE=false` + `APICURIO_USE_ID=contentId` + `PREFER_CONTENT_ID=true` only when that is your real stack.

## Prerequisites

- Docker + Docker Compose
- JDK 17+
- Maven 3.9+

## Run

```bash
cd header-wire-format-poc
chmod +x scripts/*.sh
./scripts/run-demo.sh
```

What happens:

1. Start source Kafka + Apicurio, dest Kafka + Confluent SR, Connect  
2. Produce header-format Avro on the source  
3. Confirm an Apicurio consumer can read it  
4. Export schemas (ccompat) → IMPORT into Confluent (IDs preserved)  
5. Start MirrorSource + header→wire SMT  
6. Consume on dest with stock Confluent deserializer  

Success looks like: `SUCCESS: Confluent consumer read N rewritten record(s) without Apicurio`

Optional stricter check (ID gaps, lookalike schemas):

```bash
EDGE_CASES=1 ./scripts/run-demo.sh
```

## Ports

| Service | Host |
|---|---|
| Source Kafka | `localhost:29092` |
| Dest Kafka | `localhost:29094` |
| Apicurio | `http://localhost:8081` |
| Confluent Schema Registry | `http://localhost:8082` |
| Connect | `http://localhost:8083` |

## Layout

| Path | Role |
|---|---|
| `smt/` | `ApicurioHeaderToConfluentWire` transform |
| `apps/` | Produce / consume helpers |
| `scripts/run-demo.sh` | End-to-end happy path |
| `connectors/replicator-example.json` | Replicator config sketch |
| `docker-compose.yml` | Local source + dest + Connect |

## Cleanup

```bash
docker-compose down -v
```
