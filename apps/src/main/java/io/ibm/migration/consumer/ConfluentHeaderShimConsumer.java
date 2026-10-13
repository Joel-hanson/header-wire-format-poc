package io.ibm.migration.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.ibm.migration.OrderSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Consumes header-mode Apicurio records from the Confluent-side topic using
 * {@link ApicurioHeaderShimDeserializer} (reads {@code apicurio.value.globalId}).
 */
public final class ConfluentHeaderShimConsumer {

    public static void run() {
        String bootstrap = env("DEST_BOOTSTRAP", "localhost:29094");
        String registry = env("SCHEMA_REGISTRY_URL", "http://localhost:8082");
        String topic = env("DEST_TOPIC", OrderSchema.TOPIC);
        String group = env("CONSUMER_GROUP", "cflt-header-shim-" + System.currentTimeMillis());

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ApicurioHeaderShimDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            System.out.printf(
                    "Listening on dest topic '%s' with ApicurioHeaderShimDeserializer ...%n", topic);
            long deadline = System.currentTimeMillis() + 45_000;
            int seen = 0;
            while (System.currentTimeMillis() < deadline && seen < 30) {
                for (ConsumerRecord<String, GenericRecord> r : consumer.poll(Duration.ofSeconds(2))) {
                    seen++;
                    System.out.printf(
                            "  header-shim topic=%s offset=%d key=%s value=%s%n",
                            r.topic(), r.offset(), r.key(), r.value());
                }
            }
            if (seen == 0) {
                System.err.printf(
                        "FAILED: no records decoded from '%s' with header shim.%n", topic);
                System.exit(2);
            }
            System.out.printf(
                    "SUCCESS: header shim read %d record(s) from '%s' via Confluent SR.%n",
                    seen, topic);
        }
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
