# Header wire format → Confluent via Connect SMT (Option 2 PoC)

Proof-of-concept for migrating **Apicurio `ENABLE_HEADERS=true`** topics from an Event Streams stand-in to a Confluent stand-in **without keeping Apicurio after cutover** and **without a long-lived rewrite service on the source**.

## Why this path

| Approach | Why it may not fit |
|---|---|
| **Option 1** – rewrite/republish on ES | New migration-specific pipeline to build and operate |
| **Option 3** – consumer shim | Extra deserializer for every app; you want clients on plain Confluent serde after cutover |
| **Option 2** – Replicator/Connect + SMT | Rewrite once in flight; dest topics are normal Confluent wire format; clients use stock `KafkaAvroDeserializer` |

> Note on Option 3: a shim does **not** need Apicurio at runtime if the header ID matches the ID you IMPORT into Confluent. The usual objection is app-side rollout, not Apicurio longevity. This PoC still targets Option 2 given your constraints.

## Architecture

```
[Producer + Apicurio headers] → kafka-source (ES)
                                      │
                                      │  MirrorSourceConnector*
                                      │  + ApicurioHeaderToConfluentWire SMT
                                      ▼
                              kafka-dest (Confluent)
                                      │
                                      ▼
                    [Consumer + Confluent KafkaAvroDeserializer]
                    Schema Registry (IDs imported from Apicurio ccompat)
```

\* Confluent **Replicator** is license-gated. This PoC uses Kafka **MirrorSourceConnector** with the **same SMT**.  
`connectors/replicator-example.json` shows the Replicator equivalent when you have CP.

Critical Connect settings (required for both Replicator and MM2):

- `ByteArrayConverter` for **key, value, and headers** (preserve 8-byte Apicurio ID bytes)
- SMT reads `apicurio.value.contentId` (8-byte BE long), prepends `0x00 + 4-byte id`

## Prerequisites

- Docker + Docker Compose
- JDK 17+
- Maven 3.9+

## Quick start

```bash
cd ~/header-replicator-poc
chmod +x scripts/*.sh
./scripts/run-demo.sh
```

What the demo does:

1. Starts source Kafka + Apicurio, dest Kafka + Confluent Schema Registry, Connect  
2. Produces Avro with **`ENABLE_HEADERS=true`** and **`use-id=contentId`**  
3. Consumes successfully with Apicurio on the source  
4. Migrates schemas via **ccompat → Confluent IMPORT** (ID preservation, same idea as `schemas-ccompat-export`)  
5. Starts Connect connector with the header→wire SMT  
6. Consumes on the destination with **stock Confluent deserializer** (no Apicurio)

## Manual steps

```bash
docker-compose up -d --build
./scripts/wait-for.sh
./scripts/create-topics.sh

(cd apps && mvn -q -DskipTests package)
JAR=apps/target/header-replicator-apps-1.0.0.jar

SOURCE_BOOTSTRAP=localhost:29092 APICURIO_URL=http://localhost:8081 \
  java -jar "$JAR" produce 5

SOURCE_BOOTSTRAP=localhost:29092 APICURIO_URL=http://localhost:8081 \
  java -jar "$JAR" consume-es

./scripts/migrate-schema.sh
./scripts/start-connector.sh
sleep 15

DEST_BOOTSTRAP=localhost:29094 SCHEMA_REGISTRY_URL=http://localhost:8082 \
  java -jar "$JAR" consume-cflt
```

## Ports

| Service | Host port |
|---|---|
| Source Kafka | `localhost:29092` |
| Dest Kafka | `localhost:29094` |
| Apicurio | `http://localhost:8081` |
| Confluent Schema Registry | `http://localhost:8082` |
| Connect | `http://localhost:8083` |

## Success criteria

- Source records: Apicurio headers present (`apicurio.value.contentId`, `apicurio.value.encoding=BINARY`), payload has **no** magic byte  
- Dest records: Confluent 5-byte prefix; Apicurio headers stripped  
- `consume-cflt` prints `SUCCESS: Confluent consumer read N rewritten record(s) without Apicurio`

## ID alignment (must stay true)

Producer uses `apicurio.registry.use-id=contentId` so the header ID matches default ccompat IDs and your IMPORT.  
If producers used `globalId` while ccompat exported `contentId`, the SMT would write the wrong Confluent ID — same pitfall as any rewrite/shim.

## Cleanup

```bash
docker-compose down -v
```
