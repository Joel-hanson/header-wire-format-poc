---
title: "Migrating schemas"
excerpt: "Migrate schemas from IBM Event Streams Apicurio Registry to Confluent Schema Registry with schema ID preservation."
categories: migration
slug: migrate-schemas
toc: true
---

Migrate your schemas from {{site.data.reuse.es_name}} Apicurio Registry to Confluent Schema Registry with schema ID preservation.

## Overview
{: #overview}

{{site.data.reuse.es_name}} stores schemas in an embedded Apicurio Registry. Most IBM Event Streams applications use the Apicurio 2.x native serde library (`AvroKafkaSerializer` / `AvroKafkaDeserializer`) with its default configuration, which writes an 8-byte schema ID into the message payload. Confluent expects a 4-byte schema ID. When Cluster Linking replicates topic data to Confluent, it copies those bytes unchanged, so consumers on the Confluent side cannot deserialize the messages without a consumer-side change. For consumers reading from replicated topics to deserialize messages correctly, Confluent Schema Registry must also map those same schema IDs to the same schemas.

**If your application has not yet gone live or you can update the producer before migration**, consider switching the producer to the Confluent serde (`KafkaAvroSerializer`) pointed at the Apicurio ccompat endpoint (`/apis/ccompat/v7`) before starting this migration. The Confluent serde writes a 4-byte schema ID, which Confluent can read natively after Cluster Linking replication. Migration then becomes a URL change only — no custom deserializer needed. See [Path B](#cutover-path-b).

If producers have already written 8-byte records to topics, or you cannot change the producer library before migration, use the custom deserializer approach in [Path A](#cutover-path-a) on the consumer side.

Migrating from {{site.data.reuse.es_name}} to Confluent has two parts: migrating the schemas (Steps 1–3) and updating the client applications (Step 4).

Apicurio Registry refers to a stored schema as an **artifact**. Confluent Schema Registry organizes schemas under **subjects**. During this migration, each Apicurio artifact exposed through the ccompat API is exported and imported as a Confluent subject. This documentation uses **subject** when referring to ccompat or Confluent operations.

Schema migration has two stages:

- **Export:** Use the {{site.data.reuse.es_name}} CLI to export each schema version from Apicurio in ccompat format to local JSON files. The ccompat (Confluent Schema Registry compatibility) API is an Apicurio endpoint that exposes schemas by using the same wire format as Confluent Schema Registry, which allows standard tooling to interact with Apicurio without code changes. For more information, see the [Apicurio ccompat API reference](https://www.apicur.io/registry/docs/apicurio-registry/3.3.x/getting-started/assembly-confluent-schema-registry-compatibility.html){:target="_blank"}.
- **Import:** Register those files into Confluent Schema Registry by using `IMPORT` mode, which is the only method that preserves the original schema IDs.

## Known limitations
{: #known-limitations}

The following limitations apply to this migration process:

| Limitation | Notes |
|------------|-------|
| Header-based wire format | If `ENABLE_HEADERS=true` is set on Apicurio native serdes, schema IDs are placed in Kafka message headers as 8-byte longs rather than in the payload prefix. Confluent deserializers cannot read this format directly. See [Header-based wire format](#header-wire-format) for remediation options. |
| Payload 8-byte wire format | The **Apicurio 2.x serde library** with its default configuration writes an 8-byte schema ID in the payload instead of the 4-byte ID that Confluent expects. Cluster Linking copies the bytes unchanged, and the Confluent deserializer reads the wrong schema ID. To avoid this, switch producers to the Confluent serde before migration (see [Path B](#cutover-path-b)). If records are already written with 8-byte IDs, consumers need a custom deserializer. See [Path A](#cutover-path-a) and [Payload 8-byte wire format](#payload-8byte-format). |
| `LEGACY_ID_MODE` mismatch | If `APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED=true` is set, the ccompat API exports `globalId` values instead of `contentId` values. The IDs embedded in Kafka messages are always `contentId` by default. Importing the wrong ID type breaks deserialization on replicated topics regardless of whether the schema content is correct. Verify this setting and align producer configuration before exporting. |
| Soft-deleted versions included | The ccompat API includes soft-deleted versions in its response. The export and import process treats these as live versions on Confluent. Hard-delete unwanted versions on Apicurio before exporting, or exclude the subjects. |
| `DISABLED` and `DEPRECATED` states not visible | The ccompat API returns all schemas unconditionally. Use a targeted `--name` regex to exclude disabled or deprecated subjects. |
| Circular schema references | If subjects reference each other in a cycle, no valid import order exists and the Confluent import fails. Resolve circular references in Apicurio before exporting. |
| Schemas with missing references metadata | If a schema was registered in Apicurio without reference metadata, the export cannot include the subjects it depends on, and Confluent rejects the schema on import. Re-register those schemas in Apicurio with explicit references before exporting. |
| Subject list capped at 1000 by default | The export discovers subjects with a single call to the ccompat `GET /subjects` endpoint. Apicurio limits that response with `apicurio.ccompat.max-subjects` (default `1000`) and truncates silently, so subjects beyond the limit are not exported. A larger subject list also increases response size and latency, which can reach OpenShift Route, Kubernetes Ingress, or endpoint timeout limits before the export finishes. For more information, see [exporting a large number of subjects](#export-large-subject-list). |
| Confluent Schema Registry contexts not supported | The export targets the default context only. Re-import manually into named contexts if required. |
| Schema labels and custom metadata | These have no Confluent equivalent and do not carry over. Recreate relevant tags by using [Confluent Schema Registry properties](https://docs.confluent.io/platform/current/schema-registry/schema-linking.html){:target="_blank"} after migration. |
| Apicurio version strings (such as `"1.0.0"`) | Confluent supports only sequential integer version numbers. A subject that uses non-integer version strings cannot be exported because the ccompat API cannot list its versions. Re-register the schemas in Apicurio without specifying a version so that Apicurio assigns numeric version numbers, and then export them again. Clients that use the Apicurio native v3 API to look up versions by string must replace their registry client when moving to Confluent. |

## Before you begin
{: #before-you-begin}

Complete the following checks before you begin the migration:

- **Destination registry:**

  Confirm that `mode.mutability=true` is set on the destination Confluent Schema Registry. `IMPORT` mode requires this setting. For more information, see [Schema Registry configuration](https://docs.confluent.io/platform/current/schema-registry/installation/config.html){:target="_blank"}.

- **Source registry:**

  - **Wire format:** Check whether your producers use payload mode or header mode by inspecting the headers of a message on any topic.

    - If the message has `apicurio.value.contentId` or `apicurio.value.globalId` headers, your producers use header mode. See [Header-based wire format](#header-wire-format) before continuing.
    - If there are no Apicurio headers, your producers use payload mode. Standard payload mode (a `0x00` magic byte followed by a 4-byte schema ID) needs no further action. If the schema ID is all zeros or does not resolve in Apicurio, the producer might still be using the Apicurio 2.x serde library. See [Payload 8-byte wire format](#payload-8byte-format).

  - **Legacy ID mode:** Check whether the `APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED` environment variable is set on your Apicurio Registry.

    If it is set to `true`, the export uses `globalId` instead of `contentId`, so producers must also be configured with `USE_ID=globalId`. If the configuration is mixed, contact IBM support before proceeding.

  - **Undocumented schemas:** Only schemas registered in Apicurio are exported. Audit your producers and consumers and register any missing schemas before you start.

- **CLI:**

  - The `schemas-ccompat-export` command is part of the {{site.data.reuse.es_name}} CLI plug-in for `kubectl`. [Install the {{site.data.reuse.es_name}} CLI plug-in](../../installing/post-installation/#installing-the-event-streams-command-line-interface) if not already installed.

  - {{site.data.reuse.es_cli_init_111}}

## Migration steps
{: #migration-steps}

Complete the steps described in the following sections to migrate your schemas:

### Step 1: Export schemas from {{site.data.reuse.es_name}}
{: #export-schemas}

1. Run the following command to export schemas from Apicurio to local JSON files:

   ```shell
   kubectl es schemas-ccompat-export --name ".*" --output-dir ./schema-export
   ```

   This command exports all schemas to the output directory. For each subject, it creates a subdirectory and saves each schema version as `v{version}.json` (for example, `./orders-value/v1.json`). Each file contains the Confluent-compatible payload ready for import:

   ```json
   {
     "subject": "orders-value",
     "id": 42,
     "version": 1,
     "schemaType": "AVRO",
     "schema": "{...}"
   }
   ```

   If a subject name contains any of the characters `/ \ : * ? " < > |`, each of those characters becomes `_` in the subdirectory name.

1. Review the `manifest.json` file that the command writes to the output directory. This file lists the exported subjects in a safe import order, using the original subject names:

   ```json
   {
     "subjects": ["address-value", "order-with-address-value", "order-summary-value"]
   }
   ```

   The manifest is always written, whether or not your schemas reference each other. You do not have to use it. It matters only when a schema reuses types from another subject, because Confluent rejects such a schema unless the subject it depends on is registered first. Following the manifest order is always safe, and the sample import script uses it.

1. Review the exported files and confirm that the schema IDs match what you expect before you import the schemas.

The `--name` flag accepts a regular expression (regex) that is matched against the full subject name. The expression is automatically anchored, so it must match the entire subject name, not a substring. The following table shows some example expressions:

| Regex | Matches |
|-------|---------|
| `my-topic-value` | Exact subject name |
| `orders\..*` | All subjects starting with `orders.` |
| `topic-a|topic-b` | Two specific subjects |
| `.*` | All subjects |
| `orders\..*(?<!-key)` | All subjects starting with `orders.` except those ending with `-key` |

The following table describes the available flags:

| Flag | Description |
|------|-------------|
| `--name` | Required. Regular expression to match full subject names. |
| `--output-dir` | Required. Directory where output files are created. The directory is created if it does not exist. |
| `--no-references` | Optional. Export only the subjects matched by `--name`. Referenced subjects are not included. |
| `--force` | Skip the confirmation prompt and allow writing to a non-empty directory. |

#### Exporting a large number of subjects
{: #export-large-subject-list}

If your registry has more than 1000 subjects, complete one of the following tasks before you export with `--name ".*"`:

- Raise the Apicurio subject-list limit: Set `apicurio.ccompat.max-subjects` on the Apicurio Registry to a value above your subject count. On {{site.data.reuse.es_name}}, set the equivalent environment variable on the Apicurio Registry component in the `EventStreams` custom resource, for example:

  ```yaml
  spec:
    apicurioRegistry:
      env:
        - name: APICURIO_CCOMPAT_MAX_SUBJECTS
          value: "5000"
  ```

- Export in batches: Use a narrower `--name` regex so that each run stays under the current limit.

Raising the subject-list limit increases the size and duration of the `GET /subjects` response. On OpenShift, that traffic usually goes through a Route. On other Kubernetes platforms, it goes through an Ingress or similar endpoint. Route or Ingress timeouts and per-pod connection limits on the router can then cause the export to fail, even after you raise `max-subjects`.

Use the following options to adjust Route or Ingress timeout and connection limits:

- On OpenShift, increase the Route timeout or related router annotations for the Apicurio Registry route. For example, set `haproxy.router.openshift.io/timeout`. For more information, see [Configuring route timeouts](https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/ingress_and_load_balancing/routes#nw-configuring-route-timeouts_configuring-routes){:target="_blank"} in the OpenShift documentation.
- On {{site.data.reuse.es_name}}, configure the Apicurio Registry endpoint (`spec.apicurioRegistry.endpoints`), including optional route or ingress annotations, as described in [REST services access](../../installing/configuring/#rest-services-access).
- Export in batches by using `--name` if you cannot change Route or Ingress limits in your cluster.

#### Schema references
{: #schema-references}

Most schemas are self-contained and do not depend on any other subject. If none of your schemas reuse types from another subject, this section does not apply to you, and the export and import steps work as described.

A schema can reuse types that are defined in another subject. For example, an `order` schema might use an `Address` type that belongs to the `address-value` subject. Confluent requires the referenced subject to be registered before the schema that uses it.

You do not need to list referenced subjects in `--name`. The export includes them for you, and `manifest.json` gives you an import order that satisfies the dependencies. A subject that is included only as a dependency contains just the version that is referenced, so its subdirectory might hold a single file such as `v3.json` instead of `v1.json`.

Use `--no-references` if you want only the subjects that `--name` matched. In that case, make sure that the referenced subjects already exist in Confluent, and work out the import order yourself.

If the command warns that a referenced subject is not in the registry, restore or re-register that subject in Apicurio and export again. Importing the schema without it fails.

### Step 2: Import schemas into Confluent Schema Registry
{: #import-schemas}

Use the [Confluent Schema Registry REST API](https://docs.confluent.io/platform/current/schema-registry/develop/api.html){:target="_blank"} to import each schema version by using [`IMPORT` mode](https://docs.confluent.io/platform/current/schema-registry/installation/migrate.html){:target="_blank"}.

The following steps import one subject at a time, so you can import a single subject or work through the whole export. If any of your schemas reuse types from another subject, import the subjects in the order listed in `manifest.json`. Otherwise, the order does not matter. To import everything in one run instead, see the [sample script for batch import](#batch-import-script).

**Important:** If your Confluent Schema Registry endpoint requires authentication or uses a custom TLS certificate, add the appropriate options to the `curl` commands in this section:
- For basic authentication or RBAC credentials, add `--user "<username>:<password>"`.
- For TLS validation with an internal or custom CA certificate, add `--cacert <ca-cert-path>`.
- For more information, see [securing Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/security/index.html){:target="_blank"}.

1. Record the original global compatibility value so that you can restore it after the import. Set the global compatibility to `NONE` to prevent Confluent from rejecting versions by running the following command:

   ```bash
   curl -X PUT https://<confluent-sr>/config \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     -d '{"compatibility": "NONE"}'
   ```

2. For each subject that you want to import, complete the following steps:

   a. Set the subject to `IMPORT` mode by running the following command:

      ```bash
      curl -X PUT https://<confluent-sr>/mode/<subject> \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d '{"mode": "IMPORT"}'
      ```

      **Note:** Confluent Schema Registry returns HTTP 422 if the subject already has registered versions. This occurs when a subject is pre-created before the import, and in all re-import scenarios. Pass `?force=true` to override this check on an existing subject, or delete the subject before re-importing. For more information, see [re-importing a subject](#re-import).

   b. Register each version in ascending order by using the exported JSON file as the request body. Run the following command:

      ```bash
      curl -X POST https://<confluent-sr>/subjects/<subject>/versions \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d @./orders-value/v1.json
      ```

   c. Return the subject to `READWRITE` mode by running the following command:

      ```bash
      curl -X PUT https://<confluent-sr>/mode/<subject> \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d '{"mode": "READWRITE"}'
      ```

3. After you import all subjects, restore the global compatibility to its original value by running the following command:

   ```bash
   curl -X PUT https://<confluent-sr>/config \
     -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     -d '{"compatibility": "<original_value>"}'
   ```

**Note:** If a registration returns HTTP 422 with a message such as `Subject 'address-value' not found`, the schema uses types from a subject that is not registered yet. Import that subject first, then try again. If the subject is not in `manifest.json` at all, the schema was registered in Apicurio without reference metadata. Re-register it in Apicurio with explicit references and export again.

#### Sample script for batch import
{: #batch-import-script}

For a sample script that automates the import of multiple subjects and versions, see [Event Streams Schema Registry samples](https://github.com/ibm-messaging/event-streams-samples/tree/master/schema-registry){:target="_blank"}. The script reads `manifest.json` from the export directory to process the subjects in the correct order, and exits if that file is missing.

The script does not cover every migration scenario. Review and extend the script to suit your environment and requirements before you run it.

### Step 3: Validate
{: #validate}

Confirm that each schema ID exported from Apicurio resolves correctly in Confluent before you switch any traffic to Confluent.

For each exported JSON file, take the `id` field and run the following command:

```bash
curl https://<confluent-sr>/schemas/ids/<id>
```

Verify that the response body matches the schema in the exported file. If any ID is missing or returns the wrong schema, [re-import the affected subject](#re-import) before you proceed to application migration.

#### Re-importing a subject
{: #re-import}

If a schema import fails or produces incorrect results, you can re-import the affected subject. `PUT /mode/{subject}` returns HTTP 422 if the subject already has registered versions. The following two options are available depending on the state of the subject:

- **Option A: Append new versions only**

  Use this option when the existing versions are correct and you only need to add versions that were missed or added to Apicurio after the initial export.

  Follow [step 2b in the import steps](#import-schemas) with the following changes:

  - In step 2a, append `?force=true` to the mode URL (`PUT /mode/<subject>?force=true`).
  - In step 2b, register only the new version files, not all versions. Run `GET /subjects/<subject>/versions` on Confluent to identify the versions that already exist. For example, a response of `[1,2]` means that versions 1 and 2 are already registered. Because the exported schema files are named after their version numbers (`v1.json`, `v2.json`, and so on), you only need to register `v3.json` and later files. Compare version numbers rather than the `id` field because multiple versions that contain the same schema share a single schema ID.

- **Option B: Full subject replacement**

  Use this option when the subject has versions with wrong IDs or corrupted data that cannot be corrected by appending.

  1. Soft-delete the subject by running the following command:

     ```bash
     curl -X DELETE https://<confluent-sr>/subjects/<subject>
     ```

  2. Permanently delete the subject (hard delete) by running the following command:

     ```bash
     curl -X DELETE "https://<confluent-sr>/subjects/<subject>?permanent=true"
     ```

  3. Follow [step 2 in the import steps](#import-schemas) to set the subject to `IMPORT` mode, register all versions in ascending order, and return it to `READWRITE` mode.

### Step 4: Client application updates
{: #cutover}

After all schema IDs are validated in Confluent Schema Registry, update each producer and consumer application.

The Apicurio 2.x serde writes an 8-byte schema ID into the message payload by default. If your producers use this serde without any explicit configuration (the most common IBM Event Streams setup), your topics contain 8-byte records. Confluent's `KafkaAvroDeserializer` cannot read them directly after Cluster Linking replication. **Start with [Path A](#cutover-path-a) before updating producers.** If your topics already use the 4-byte Confluent wire format, go directly to [Path B](#cutover-path-b).

#### Path A: Topics with 8-byte records — write a custom deserializer (do this first)
{: #cutover-path-a}

**Who this applies to:** Any consumer reading a topic where the Apicurio 2.x serde was used with default configuration. This is the default IBM Event Streams setup. If even one producer wrote 8-byte records to a topic, every consumer of that topic needs this change regardless of which library the consumer uses.

You need to write a custom deserializer that wraps `KafkaAvroDeserializer`. The custom deserializer detects the 8-byte prefix, extracts the real 4-byte schema ID from it, rewrites the prefix to the standard Confluent format in memory, and then delegates to the inner `KafkaAvroDeserializer`. It must also pass records that already have a 4-byte prefix through unchanged, so a single consumer can safely read a topic that contains both formats.

**How to detect the 8-byte format**

The Apicurio 2.x `DefaultIdHandler` writes:

```
byte[0]    = 0x00                         (magic byte, same as Confluent)
byte[1..4] = 0x00 0x00 0x00 0x00          (high 4 bytes of the 8-byte long, always zero)
byte[5..8] = actual schema ID (4 bytes)
byte[9..]  = Avro binary body
```

Because schema ID 0 is never assigned by any registry, `bytes[1..4] == 0x00000000` unambiguously identifies an 8-byte Apicurio record. A Confluent-format record always has a non-zero value at `bytes[1..4]`.

**Reframe logic to implement**

```
If payload.length >= 9 AND bytes[1..4] == 0x00000000:
    realId   = bytes[5..8]
    reframed = [ 0x00 ] + realId + bytes[9..]   ← standard Confluent 5-byte prefix
    delegate reframed to KafkaAvroDeserializer
Else:
    delegate payload unchanged to KafkaAvroDeserializer
```

**Java implementation gist**

`KafkaAvroDeserializer` is a class, not an interface, so implement `Deserializer<Object>` and delegate to an inner instance. Do not try to extend `KafkaAvroDeserializer` directly.

```java
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import java.nio.ByteBuffer;
import java.util.Map;

public class ApicurioPayloadDeserializer implements Deserializer<Object> {

    private final KafkaAvroDeserializer inner = new KafkaAvroDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Forward all schema.registry.* properties to the inner deserializer unchanged.
        inner.configure(configs, isKey);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return inner.deserialize(topic, reframe(data));
    }

    @Override
    public void close() {
        inner.close();
    }

    /**
     * Rewrites the Apicurio 8-byte payload prefix to the Confluent 4-byte prefix.
     * Passes records that already have a 4-byte prefix through unchanged.
     */
    static byte[] reframe(byte[] data) {
        if (data == null || data.length < 5 || data[0] != 0x00) return data;

        int firstFour = ByteBuffer.wrap(data, 1, 4).getInt();

        if (firstFour == 0 && data.length >= 9) {
            // 8-byte framing detected: real schema ID is in bytes[5..8].
            ByteBuffer out = ByteBuffer.allocate(data.length - 4);
            out.put((byte) 0x00);              // magic byte
            out.put(data, 5, 4);               // real 4-byte schema ID
            out.put(data, 9, data.length - 9); // Avro body unchanged
            return out.array();
        }
        // Non-zero firstFour: already Confluent 4-byte format, pass through.
        return data;
    }
}
```

Register your custom deserializer in the consumer configuration and point it at Confluent Schema Registry. All `schema.registry.*` properties are forwarded to the inner `KafkaAvroDeserializer` by the `configure` method above.

```java
// Before: Apicurio 2.x serde, default config (8-byte payload)
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    AvroKafkaDeserializer.class.getName());
props.put(SerdeConfig.REGISTRY_URL,
    "https://my-es-cluster.example.com/apis/registry/v2");
```

```java
// After: your custom deserializer
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    "com.example.ApicurioPayloadDeserializer"); // your package
props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
    "https://schema-registry.confluent.svc:8081");
```

This change is transitional. Once all 8-byte records have aged out under the topic retention policy, replace the custom deserializer with `KafkaAvroDeserializer` directly. On compacted topics, 8-byte records persist until every key is rewritten by a producer on the new serde, so keep the custom deserializer until that is confirmed.

#### Path B: Confluent serde in use or switching before migration (URL change only)
{: #cutover-path-b}

**Who this applies to:** Producers and consumers that already use `io.confluent:kafka-avro-serializer`, or applications that can switch to it before migration. The Confluent serde can point at the Apicurio ccompat endpoint (`/apis/ccompat/v7`) before migration and then switch to Confluent Schema Registry — the only change between the two is the URL and credentials.

**Why switching to the Confluent serde before migration simplifies things:** The Confluent serde writes a 4-byte schema ID, which is the format Confluent Schema Registry natively expects. Topics written with this serde need no custom deserializer after Cluster Linking replication. If you are still on the Apicurio native serde and can update the producer before topics are migrated, this is the lowest-effort migration path.

| Setting | Before (IBM Event Streams) | After (Confluent) |
|---|---|---|
| Registry URL | `https://<es-host>/apis/ccompat/v7` | `https://<confluent-sr>:<port>` |
| Auth credentials | {{site.data.reuse.es_name}} API key / SCRAM | Confluent SR API key or mTLS cert |
| CA certificate | {{site.data.reuse.es_name}} cluster CA | Confluent cluster CA |

**Java**

```java
// Before — IBM Event Streams
props.put("schema.registry.url",
    "https://my-es-cluster.example.com/apis/ccompat/v7");
props.put("schema.registry.ssl.truststore.location", "/path/to/es-truststore.jks");
props.put("schema.registry.ssl.truststore.password",  "es-password");
props.put("basic.auth.credentials.source",  "USER_INFO");
props.put("basic.auth.user.info", "apikey:es-api-secret");

// After — Confluent Schema Registry
props.put("schema.registry.url",
    "https://schema-registry.confluent.svc:8081");
props.put("schema.registry.ssl.truststore.location", "/path/to/confluent-truststore.jks");
props.put("schema.registry.ssl.truststore.password",  "confluent-password");
props.put("basic.auth.credentials.source",  "USER_INFO");
props.put("basic.auth.user.info", "sr-api-key:sr-api-secret");
```

The serializer class, topic name, Avro schema, and all other Kafka configuration remain unchanged.

**Note:** If the topic also contains 8-byte records (see [Path A](#cutover-path-a)), the consumer side still needs the shim even though the producer is already on the Confluent serde.

#### Dependency changes
{: #cutover-dependencies}

**Java (Maven)**

Add the `kafka-avro-serializer` dependency:

```xml
<dependency>
  <groupId>io.confluent</groupId>
  <artifactId>kafka-avro-serializer</artifactId>
  <version>7.7.1</version>
</dependency>
```

Consumers that use the custom deserializer described in [Path A](#cutover-path-a) must include it on their classpath. The class is part of your own application code — there is no separate library to add for the reframe logic itself.

Add the Confluent Maven repository if it is not already present:

```xml
<repository>
  <id>confluent</id>
  <url>https://packages.confluent.io/maven/</url>
</repository>
```

#### Cutover sequence
{: #cutover-sequence}

To minimise risk, cut over producers and consumers separately rather than all at once.

1. **Prepare.** Confirm that all schema IDs present in the topic resolve correctly in Confluent Schema Registry ([Step 3](#validate)). Any ID that does not resolve causes consumers to fail immediately after the switch. Also confirm which topics contain 8-byte records (see [Before you begin](#before-you-begin)), because their consumers need the custom deserializer from [Path A](#cutover-path-a).

2. **Switch consumers first.** Update each consumer to read from the mirror topic with Confluent Schema Registry. Consumers of topics with 8-byte records use the custom deserializer ([Path A](#cutover-path-a)). All other consumers use `KafkaAvroDeserializer` directly. Producers keep writing to the {{site.data.reuse.es_name}} topic and Cluster Linking keeps mirroring, so no messages are lost.

3. **Verify consumers.** Confirm that all consumer groups are processing messages from the Confluent-side topic without deserialization errors. Monitor consumer lag and error rates.

4. **Switch producers.** Update each producer to use the Confluent serde and write directly to the Confluent cluster. You can then stop Cluster Linking. From this point, producers write only 4-byte records.

5. **Remove the custom deserializer.** After the topic retention period has passed and all 8-byte records have aged out, replace the custom deserializer with `KafkaAvroDeserializer` in each consumer. On compacted topics, 8-byte records persist until every key is rewritten by a producer on the new serde, so keep the custom deserializer on those consumers until that is confirmed.

**Note:** If a producer registers a new schema version in Apicurio between the export and its cutover, re-export the affected subject and use [option A](#re-import) to append the new version to Confluent Schema Registry before the producer switches over. Use [option B](#re-import) only if the subject needs a full replacement.

---

## Header-based wire format
{: #header-wire-format}

When Apicurio native serializers run with `ENABLE_HEADERS=true`, the schema ID is not written into the message payload. Instead, it is placed in a Kafka message header as an 8-byte big-endian long:

```
Kafka headers:
  apicurio.value.contentId  =  00 00 00 00 00 00 00 05  (8 bytes, Java long)
  apicurio.value.encoding   =  BINARY

Message value (payload):
  Raw Avro binary — no magic byte, no schema ID prefix
```

The Confluent `KafkaAvroDeserializer` looks for the schema ID in the first 5 bytes of the payload (magic byte `0x00` + 4-byte int). When it encounters a payload that starts with a raw Avro byte rather than `0x00`, it throws a `SerializationException`. Cluster Linking copies message bytes unchanged, so this problem persists on mirror topics.

Schema migration (steps 1–2 above) still works for header-mode topics: the ccompat API exports the schema content and ID regardless of how producers write messages. The gap is on the **consumer side** — Confluent deserializers cannot read the messages after replication without a wire format translation step.

The recommended solution is the Kafka Connect SMT described below. Two fallback options are also available for estates where the SMT is not suitable.

### Option 1 (recommended): Kafka Connect SMT during replication
{: #header-wire-format-smt}

A custom Kafka Connect Single Message Transform (SMT) can rewrite each message in flight as Cluster Linking or MirrorMaker 2 replicates it. The SMT reads the 8-byte ID from the Apicurio header, narrows it to a 4-byte int, prepends the Confluent magic byte `0x00`, and writes the resulting 5-byte prefix to the message payload — all without calling any registry.

**What the SMT does and does not do**

- It translates the wire format: header-mode Apicurio messages become standard Confluent payload-format messages on the destination topic.
- It does not change schema IDs. The ID written into the Confluent payload is the same integer that was in the Apicurio header.
- It does not call the schema registry at runtime. The ID translation is purely byte manipulation.
- It requires that schema IDs are already imported into Confluent Schema Registry (steps 1–2 above) before replication starts. If Confluent does not know the ID, consumers still fail.

**The matched-pair constraint**

The Apicurio server setting `APICURIO_CCOMPAT_LEGACY_ID_MODE_ENABLED` controls which ID type the ccompat API exports, and therefore which ID type is imported into Confluent. The producer setting `USE_ID` controls which ID type appears in the message header. These two must be aligned:

| `LEGACY_ID_MODE_ENABLED` on ES | ccompat exports | Producer `USE_ID` | Header written | SMT `prefer.content.id` | Result |
|---|---|---|---|---|---|
| `false` (default, ES 11.4+) | `contentId` | `contentId` (default) | `apicurio.value.contentId` | `true` | ✅ Works — IDs match |
| `true` (legacy, ES ≤ 11.3) | `globalId` | `globalId` | `apicurio.value.globalId` | `false` | ✅ Works — IDs match |
| `true` | `globalId` | `contentId` | `apicurio.value.contentId` | `true` | ❌ Breaks — Confluent has globalId, payload carries contentId |
| `false` | `contentId` | `globalId` | `apicurio.value.globalId` | `false` | ❌ Breaks — Confluent has contentId, payload carries globalId |

Before you deploy the SMT, verify that the server `LEGACY_ID_MODE_ENABLED` setting and the producer `USE_ID` setting match. If they differ, align them on the producer side before migration.

**SMT fallback behaviour**

If `prefer.content.id=true` but only `apicurio.value.globalId` is present in the headers (no contentId header), the SMT falls back to the globalId value. This allows messages from producers that write only globalId to still be processed when the configuration is otherwise aligned.

**Connector configuration**

Add the following transform to your MirrorSourceConnector or Confluent Replicator connector configuration:

```json
{
  "transforms": "headerToWire",
  "transforms.headerToWire.type": "io.ibm.migration.smt.ApicurioHeaderToConfluentWire$Value",
  "transforms.headerToWire.prefer.content.id": "true",
  "transforms.headerToWire.strip.apicurio.headers": "true"
}
```

Set `prefer.content.id` to `true` when `LEGACY_ID_MODE_ENABLED=false` (default), or `false` when `LEGACY_ID_MODE_ENABLED=true`.

The following table describes all available SMT configuration properties:

| Property | Default | Description |
|----------|---------|-------------|
| `prefer.content.id` | `true` | When `true`, reads `apicurio.value.contentId` first and falls back to `apicurio.value.globalId`. Set to `false` to prefer globalId. Must match the ID type imported into Confluent. |
| `strip.apicurio.headers` | `true` | Removes all `apicurio.*` headers from the replicated message. Set to `false` to preserve them. |
| `content.id.header` | `apicurio.value.contentId` | Header name to read for the contentId. Override if a custom Apicurio headers handler is in use. |
| `global.id.header` | `apicurio.value.globalId` | Header name to read for the globalId. |
| `applies.to` | `value` | Which part of the record to rewrite: `value` or `key`. |

**Limitations of the SMT approach**

- Connect-based replication does not guarantee the same offset preservation as Cluster Linking. Confluent Replicator does not preserve consumer group offsets in the same way as Cluster Linking. In the proof of concept, MirrorMaker 2 with `IdentityReplicationPolicy` produced identical offsets on a single-partition topic without compaction, but this was not tested with Replicator, multiple partitions, or compacted topics. Evaluate offset handling as part of your cutover plan.
- The SMT only handles `ENABLE_HEADERS=true` (header mode). It does not handle the 8-byte payload format that can be produced by client applications still running the Apicurio 2.x serde library with `ENABLE_HEADERS=false` and `DefaultIdHandler`. See [Payload 8-byte wire format](#payload-8byte-format) for that case.
- If a message has no Apicurio ID header and does not start with the Confluent magic byte `0x00`, the SMT passes it through unchanged.

### Option 2: Fix-and-republish on the IBM Event Streams side
{: #header-wire-format-republish}

A Kafka Streams or Kafka Connect job reads from the header-mode source topic, moves the schema ID from the Apicurio header into the standard payload prefix (`0x00` + 4-byte int), and writes each message to a new topic. Cluster Linking then replicates that new topic normally without any SMT.

**When to consider this option:**

- You cannot use a custom SMT in the replication connector (for example, a managed replication service with no SMT extension point).
- You want the replication layer to remain unmodified and prefer all transformation to happen at the source.
- You need to keep the original header-mode topic intact for other consumers that still use the Apicurio native serde.

**Drawbacks:**

- An extra topic and a running transformation job must be managed for the duration of the migration window, and cleaned up afterward.
- The new topic has its own offsets, so source offsets and consumer group positions do not carry over. Plan the consumer starting position as part of cutover.
- All consumers on the destination cluster must be pointed at the new topic, not the original replicated topic.
- Additional storage and processing costs for the transformation topic.

### Option 3: Consumer-side deserializer shim
{: #header-wire-format-shim}

A custom deserializer checks for the `apicurio.value.contentId` header first. If it is present, the deserializer resolves the schema ID from the header against the schema registry and deserializes the raw Avro payload directly. If no Apicurio header is found, it delegates to the standard Confluent `KafkaAvroDeserializer`.

**When to consider this option:**

- Re-producing or transforming messages is not feasible because the source topics are immutable or the migration window does not allow it.
- You have only a small number of consumer applications that need updating.
- You need to support a mixed topic where some records are in header mode and others are in standard payload mode.

**Drawbacks:**

- This option depends on Cluster Linking (or an equivalent byte-for-byte replication) to copy the unmodified header-mode records, because it does not rewrite messages.
- Every consumer application must include and configure the custom deserializer shim. For estates with many consuming applications, this is operationally heavier than deploying a single SMT.
- The shim requires continued connectivity to a schema registry (Apicurio or Confluent) that can resolve the header IDs. If the Apicurio instance is decommissioned before all header-mode records age out of the topic, the shim breaks.
- This approach does not rewrite messages, so the header-mode records remain on the topic indefinitely.

### Choosing an option
{: #header-wire-format-choosing}

In this table, **offsets preserved** means that each record has the same offset on the source and destination topics, so consumer group positions can carry over. **Consumer code changes** refers to changes that the option itself requires, beyond the registry and serde changes described in [Step 4](#cutover) that every Apicurio native serde consumer needs.

| Option | Replication tool required | Offsets preserved | Extra topic needed | Consumer code changes | Suited to many consumers |
|--------|---------------------------|-------------------|--------------------|-----------------------|--------------------------|
| [1 — SMT (recommended)](#header-wire-format-smt) | MirrorMaker 2 or Confluent Replicator (not Cluster Linking) | Conditional: verify for your connector and topic layout | No | None | Yes |
| [2 — Fix-and-republish](#header-wire-format-republish) | Cluster Linking, on the new topic | No: the new topic has its own offsets | Yes | Point each consumer at the new topic | Partly: one job, but every consumer is repointed |
| [3 — Consumer shim](#header-wire-format-shim) | Cluster Linking | Yes: records are not rewritten | No | Add the custom deserializer to every consumer | No |

---

## Payload 8-byte wire format
{: #payload-8byte-format}

Upgrading the registry server to Apicurio 3.x does not change the serde library inside your applications. A producer that still uses the Apicurio 2.x serde (`apicurio-registry-serdes-avro-serde` 2.x) with `ENABLE_HEADERS=false` writes the schema ID as an 8-byte long (`0x00` + 8 bytes) instead of the standard `0x00` + 4-byte int. Both formats start with `0x00`, so the messages replicate unchanged and Confluent deserializers read the wrong ID.

```
Source record (Apicurio 2.x DefaultIdHandler):
  byte[0]    = 0x00           ← magic byte (same as Confluent)
  byte[1..8] = 00 00 00 00 00 00 00 01  ← schema ID as 8-byte big-endian long
  byte[9..]  = <Avro binary body>

Confluent KafkaAvroDeserializer reads bytes[1..4] = 0x00000000 = ID 0
→ "Schema 0 not found"  ← runtime error
```

Producers on the 3.x serde, or on the 2.x serde with `ID_HANDLER=io.apicurio.registry.serde.Legacy4ByteIdHandler`, already write the 4-byte format and are not affected.

**Important:** The Apicurio 2.x serde source declares `ENABLE_HEADERS_DEFAULT = true` as a constant, but the actual runtime default when no `ENABLE_HEADERS` property is set is **payload mode** (`ENABLE_HEADERS=false`). Applications that set no properties explicitly produce 8-byte payload records, not header-mode records.

Fixing 8-byte records has two parts:

- **Records already in the topic:** These cannot be changed by updating producers. With Cluster Linking, consumers read them by using the deserializer shim ([Option 3](#payload-8byte-shim)). With MirrorMaker 2 or Confluent Replicator, you can rewrite them during replication instead ([Option 2](#payload-8byte-smt)).
- **New records:** These stop being 8-byte when producers move to the Confluent serde in [Path B](#cutover-path-b). If you need to stop them earlier, reconfigure the producers ([Option 1](#payload-8byte-producer-fix)).

### Option 1: Stop producers writing 8-byte records
{: #payload-8byte-producer-fix}

If producers cannot move to the Confluent serde yet, apply one of these changes so that they write 4-byte records:

- **Upgrade the serde dependency to 3.x.** The Apicurio 3.x serde writes the standard 4-byte Confluent-compatible prefix by default.
- **Set `ID_HANDLER=io.apicurio.registry.serde.Legacy4ByteIdHandler`** in the producer configuration. This makes the 2.x serde write a 4-byte prefix, which Confluent can read natively without any shim.

This change affects new records only. 8-byte records already in the topic remain, so consumers still need the shim ([Option 3](#payload-8byte-shim)) until those records age out.

### Option 2: SMT rewrite during replication (records already on disk)
{: #payload-8byte-smt}

For records that are already written with 8-byte IDs and cannot be re-produced, the `ApicurioHeaderToConfluentWire` SMT supports a `payload.id.length=8` mode that rewrites the prefix in flight during replication.

**Limitations:**
- Requires MirrorMaker 2 or Confluent Replicator. Cluster Linking does not support SMTs.
- Only corrects records written before the SMT was applied. Records produced after the SMT is deployed reach the destination in their original form (which is correct if the producer has been updated to 4-byte mode, or in 8-byte form if not).

**Connector configuration:**

```json
{
  "transforms": "payloadReframe",
  "transforms.payloadReframe.type": "io.ibm.migration.smt.ApicurioHeaderToConfluentWire$Value",
  "transforms.payloadReframe.payload.id.length": "8"
}
```

### Option 3 (recommended for Cluster Linking): Consumer-side deserializer shim
{: #payload-8byte-shim}

When using Cluster Linking (which copies bytes unchanged without SMT support), the cleanest fix is a consumer-side deserializer shim. The shim applies the same `OptimisticFallback` heuristic that Apicurio uses internally: if `bytes[1..4] == 0x00000000` and the payload is at least 9 bytes long, the actual schema ID is in `bytes[5..8]`. The shim reframes those bytes to a standard Confluent 5-byte prefix in memory before delegating to `KafkaAvroDeserializer`.

**Reframe logic:**

```
Input  (8-byte Apicurio framing):
  0x00  [00 00 00 00]  [00 00 00 01]  <avro body>
         bytes[1..4]   bytes[5..8]
         == 0          real ID = 1
         → 8-byte framing detected

Output (Confluent 4-byte framing, passed to KafkaAvroDeserializer):
  0x00  [00 00 00 01]  <avro body>
         4-byte ID = 1
```

The shim also handles already-migrated records (3.x serde or `Legacy4ByteIdHandler`) transparently — their `bytes[1..4]` is non-zero, so they pass through unchanged. This makes the shim safe to deploy before all producers have been updated.

**Why `bytes[1..4] == 0` is always the correct signal:**

Schema ID 0 is never assigned by any registry (Confluent SR, Apicurio, or otherwise). IDs start at 1. So `bytes[1..4] == 0x00000000` in an 8-byte Apicurio record is unambiguous — it can never be a legitimate Confluent-format record.

**Shim implementation gist:**

```java
public static byte[] reframe(byte[] data) {
    if (data == null || data.length < 5 || data[0] != 0x00) return data;

    int firstFour = ByteBuffer.wrap(data, 1, 4).getInt();

    if (firstFour == 0 && data.length >= 9) {
        // 8-byte framing: real ID is in bytes[5..8].
        // Build Confluent 5-byte prefix: 0x00 + bytes[5..8] + bytes[9..]
        ByteBuffer out = ByteBuffer.allocate(data.length - 4);
        out.put((byte) 0x00);             // magic
        out.put(data, 5, 4);              // low 4 bytes = real ID
        out.put(data, 9, data.length - 9); // Avro body unchanged
        return out.array();
    }
    // firstFour != 0 → already Confluent 4-byte format; pass through.
    return data;
}
```

The full `ApicurioPayloadDeserializer` class (which wraps this logic in a `Deserializer<Object>` that delegates to the inner `KafkaAvroDeserializer`) is available in the [`07-payload8-consumer-shim-poc`](../../07-payload8-consumer-shim-poc/) proof-of-concept.

**When to use each option:**

| Situation | Recommended option |
|-----------|-------------------|
| Using Cluster Linking, topic contains 8-byte records | Option 3 (consumer shim) on every consumer of the topic |
| Using MirrorMaker 2 or Confluent Replicator, topic contains 8-byte records | Option 2 (SMT rewrite), so consumers can use `KafkaAvroDeserializer` directly |
| Producers must stop writing 8-byte records before they move to the Confluent serde | Option 1, together with Option 2 or Option 3 for the records already in the topic |

**Note:** This section applies only to records produced with the Apicurio 2.x serde using `DefaultIdHandler` (the runtime default when no `ENABLE_HEADERS` or `ID_HANDLER` property is set). It does not apply to records produced with the Confluent serde, the Apicurio 3.x serde, or the Apicurio 2.x serde with `Legacy4ByteIdHandler`.
