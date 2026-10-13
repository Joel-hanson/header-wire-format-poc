package io.ibm.migration.consumer;

import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.config.IdOption;
import io.ibm.migration.OrderSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/** Pre-migration consumer: Apicurio deserializer reading schema ID from headers. */
public final class ApicurioHeaderConsumer {

    public static void run() {
        String bootstrap = env("SOURCE_BOOTSTRAP", "localhost:29092");
        String registry = env("APICURIO_URL", "http://localhost:8081");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "es-header-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer.class.getName());
        props.put(SerdeConfig.REGISTRY_URL, registry + "/apis/registry/v2");
        props.put(SerdeConfig.ENABLE_HEADERS, "true");
        props.put(SerdeConfig.USE_ID, IdOption.contentId.name());

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(OrderSchema.TOPIC));
            System.out.println("Listening on source topic '" + OrderSchema.TOPIC + "' with Apicurio header mode...");
            long deadline = System.currentTimeMillis() + 30_000;
            int seen = 0;
            while (System.currentTimeMillis() < deadline && seen < 20) {
                for (ConsumerRecord<String, GenericRecord> r : consumer.poll(Duration.ofSeconds(2))) {
                    seen++;
                    System.out.printf(
                            "es-consume offset=%d key=%s value=%s headers=%s%n",
                            r.offset(), r.key(), r.value(), headersSummary(r));
                }
            }
            System.out.println("Apicurio consumer finished, records=" + seen);
        }
    }

    private static String headersSummary(ConsumerRecord<?, ?> r) {
        StringBuilder sb = new StringBuilder();
        r.headers().forEach(h -> {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(h.key());
        });
        return sb.toString();
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? defaultValue : v;
    }
}
