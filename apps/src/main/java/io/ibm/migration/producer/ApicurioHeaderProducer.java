package io.ibm.migration.producer;

import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.apicurio.registry.serde.config.IdOption;
import io.ibm.migration.OrderSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Produces Avro with configurable Apicurio serde settings.
 *
 * <p>Env vars (all optional — defaults match Apicurio 2.x header mode):
 * <ul>
 *   <li>{@code APICURIO_ENABLE_HEADERS} — {@code true} (default) or {@code false} (payload mode)</li>
 *   <li>{@code APICURIO_USE_ID} — {@code globalId} (default) or {@code contentId}</li>
 *   <li>{@code APICURIO_ID_HANDLER} — IdHandler class when headers are off (serde default if unset)</li>
 *   <li>{@code APICURIO_SUBJECT} — optional explicit artifact ID instead of topic-derived subject</li>
 * </ul>
 *
 * <p>Prints every {@code apicurio.*} header on each record (name + byte length).
 */
public final class ApicurioHeaderProducer {

    public static void run(int count) throws Exception {
        run(count, false);
    }

    /** @param v2 produce with the evolved v2 schema (new Apicurio version → new schema ID). */
    public static void run(int count, boolean v2) throws Exception {
        String bootstrap = env("SOURCE_BOOTSTRAP", "localhost:29092");
        String registry  = env("APICURIO_URL", "http://localhost:8081");

        // Defaults match Apicurio 2.x / Event Streams clients and SMT prefer.content.id=false.
        // Set APICURIO_USE_ID=contentId (and PREFER_CONTENT_ID=true on the connector) for 3.x-style IDs.
        // "default" leaves the property unset so the serde's own default applies.
        String  enableHeadersEnv = env("APICURIO_ENABLE_HEADERS", "true");
        String  useId         = env("APICURIO_USE_ID", IdOption.globalId.name());
        boolean enableHeaders = enableHeadersEnv.equalsIgnoreCase("default")
                ? true : Boolean.parseBoolean(enableHeadersEnv);
        String  idHandler     = env("APICURIO_ID_HANDLER", "");
        String  subject       = env("APICURIO_SUBJECT", "");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(SerdeConfig.REGISTRY_URL, registry + "/apis/registry/v2");
        props.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, "true");
        if (!enableHeadersEnv.equalsIgnoreCase("default")) {
            props.put(SerdeConfig.ENABLE_HEADERS, String.valueOf(enableHeaders));
        }
        if (!useId.equalsIgnoreCase("default")) {
            props.put(SerdeConfig.USE_ID, useId);
        }
        props.put(SerdeConfig.ARTIFACT_RESOLVER_STRATEGY,
                "io.apicurio.registry.serde.strategy.TopicIdStrategy");
        if (!subject.isBlank()) {
            // EXPLICIT_ARTIFACT_ID overrides the strategy's artifact ID so the serde
            // resolves the schema under this subject name instead of deriving it from the topic.
            props.put(SerdeConfig.EXPLICIT_ARTIFACT_ID, subject);
        }
        if (!idHandler.isBlank()) {
            props.put(SerdeConfig.ID_HANDLER, idHandler);
        }

        System.out.printf("producer config: ENABLE_HEADERS=%s USE_ID=%s ID_HANDLER=%s SUBJECT=%s%n",
                enableHeadersEnv.equalsIgnoreCase("default") ? "(serde default)" : enableHeaders,
                useId.equalsIgnoreCase("default") ? "(serde default)" : useId,
                idHandler.isBlank() ? "(serde default)" : idHandler,
                subject.isBlank() ? "(from topic)" : subject);

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= count; i++) {
                GenericRecord order = v2
                        ? OrderSchema.orderV2("ORD-V2-" + i, "customer-" + i, 10.0 * i, "EUR")
                        : OrderSchema.order("ORD-" + i, "customer-" + i, 10.0 * i);
                ProducerRecord<String, GenericRecord> record =
                        new ProducerRecord<>(OrderSchema.TOPIC, (v2 ? "v2-key-" : "key-") + i, order);
                var meta = producer.send(record).get(30, TimeUnit.SECONDS);

                // Collect all apicurio.* headers for diagnostics.
                List<String> headerSummary = new ArrayList<>();
                for (Header h : record.headers()) {
                    if (h.key().startsWith("apicurio.")) {
                        byte[] val = h.value();
                        String repr = val == null ? "null"
                                : val.length == 8 ? "8-byte-long=" + ByteBuffer.wrap(val).getLong()
                                : val.length == 4 ? "4-byte-int=" + ByteBuffer.wrap(val).getInt()
                                : new String(val) + "(" + val.length + "B)";
                        headerSummary.add(h.key() + "=" + repr);
                    }
                }

                System.out.printf(
                        "produced topic=%s partition=%d offset=%d orderId=%s headers=[%s]%n",
                        meta.topic(), meta.partition(), meta.offset(),
                        order.get("orderId"),
                        String.join(", ", headerSummary));
            }
            producer.flush();
        }
        System.out.printf("Done. ENABLE_HEADERS=%s USE_ID=%s%n", enableHeaders, useId);
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? defaultValue : v;
    }
}
