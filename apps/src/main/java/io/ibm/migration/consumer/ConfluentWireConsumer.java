package io.ibm.migration.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
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
 * Post-migration consumer: standard Confluent KafkaAvroDeserializer.
 * Expects Confluent wire format (magic + 4-byte ID) after the SMT rewrite.
 * Does NOT talk to Apicurio.
 */
public final class ConfluentWireConsumer {

    public static void run() {
        String bootstrap = env("DEST_BOOTSTRAP", "localhost:29094");
        String registry = env("SCHEMA_REGISTRY_URL", "http://localhost:8082");
        // IdentityReplicationPolicy keeps the topic name as "orders".
        String topic = env("DEST_TOPIC", OrderSchema.TOPIC);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cflt-wire-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registry);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            System.out.println("Listening on dest topic '" + topic + "' with Confluent deserializer (no Apicurio)...");
            long deadline = System.currentTimeMillis() + 45_000;
            int seen = 0;
            while (System.currentTimeMillis() < deadline && seen < 20) {
                for (ConsumerRecord<String, GenericRecord> r : consumer.poll(Duration.ofSeconds(2))) {
                    seen++;
                    System.out.printf(
                            "cflt-consume offset=%d key=%s value=%s valueSize=%d%n",
                            r.offset(), r.key(), r.value(), r.serializedValueSize());
                }
            }
            if (seen == 0) {
                System.err.println("FAILED: no records consumed from Confluent side. "
                        + "Check SMT connector, schema IMPORT, and topic name.");
                System.exit(2);
            }
            System.out.println("SUCCESS: Confluent consumer read " + seen
                    + " rewritten record(s) without Apicurio.");
        }
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? defaultValue : v;
    }
}
