package io.ibm.migration.verify;

import io.ibm.migration.OrderSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Dump the first record as raw bytes so we can confirm framing without any deserializer
 * interpreting headers or payload for us.
 */
public final class InspectRawRecord {

    public static int run() {
        String bootstrap = env("SOURCE_BOOTSTRAP", "localhost:29092");
        String topic = env("KAFKA_TOPIC", OrderSchema.TOPIC);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "inspect-raw-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> r : records) {
                    printRecord(r);
                    return 0;
                }
            }
        }
        System.err.println("ERROR: no records found on topic " + topic);
        return 1;
    }

    private static void printRecord(ConsumerRecord<String, byte[]> r) {
        System.out.printf("topic=%s partition=%d offset=%d key=%s%n",
                r.topic(), r.partition(), r.offset(), r.key());
        System.out.println();
        System.out.println("=== Kafka headers ===");
        boolean anyApicurio = false;
        for (Header h : r.headers()) {
            byte[] v = h.value();
            String detail;
            if (v == null) {
                detail = "null";
            } else if (v.length == 8) {
                detail = "len=8 long=" + ByteBuffer.wrap(v).getLong() + " hex=" + hex(v);
            } else if (v.length == 4) {
                detail = "len=4 int=" + ByteBuffer.wrap(v).getInt() + " hex=" + hex(v);
            } else {
                detail = "len=" + v.length + " utf8=" + new String(v) + " hex=" + hex(v);
            }
            System.out.printf("  %s => %s%n", h.key(), detail);
            if (h.key().startsWith("apicurio.")) {
                anyApicurio = true;
            }
        }
        if (!anyApicurio) {
            System.out.println("  (no apicurio.* headers)");
        }

        byte[] payload = r.value();
        System.out.println();
        System.out.println("=== Payload ===");
        if (payload == null) {
            System.out.println("  null");
            return;
        }
        System.out.printf("  length=%d%n", payload.length);
        System.out.printf("  first16=%s%n", hex(payload, Math.min(16, payload.length)));
        if (payload.length > 0 && payload[0] == 0x00) {
            System.out.println("  byte[0]=0x00 → looks like PAYLOAD mode (magic byte present)");
            if (payload.length >= 9) {
                long as8 = ByteBuffer.wrap(payload, 1, 8).getLong();
                int as4 = ByteBuffer.wrap(payload, 1, 4).getInt();
                System.out.printf("  bytes[1..8] as long = %d%n", as8);
                System.out.printf("  bytes[1..4] as int  = %d%n", as4);
            }
        } else {
            System.out.println("  byte[0]!=0x00 → looks like HEADER mode (raw Avro, no ID prefix)");
        }

        boolean hasContent = hasHeader(r, "apicurio.value.contentId");
        boolean hasGlobal = hasHeader(r, "apicurio.value.globalId");
        System.out.println();
        System.out.println("=== Verdict ===");
        System.out.printf("  apicurio.value.contentId present: %s%n", hasContent);
        System.out.printf("  apicurio.value.globalId  present: %s%n", hasGlobal);
        if (hasGlobal && !hasContent && (payload.length == 0 || payload[0] != 0x00)) {
            System.out.println("  RESULT: ENABLE_HEADERS default path → header=globalId, payload=raw Avro");
        } else if (hasContent && !hasGlobal && (payload.length == 0 || payload[0] != 0x00)) {
            System.out.println("  RESULT: header mode with USE_ID=contentId → header=contentId, payload=raw Avro");
        } else if (!hasGlobal && !hasContent && payload.length > 0 && payload[0] == 0x00) {
            System.out.println("  RESULT: payload mode (ENABLE_HEADERS=false)");
        } else {
            System.out.println("  RESULT: unexpected / mixed framing — inspect dump above");
        }
    }

    private static boolean hasHeader(ConsumerRecord<?, ?> r, String name) {
        return r.headers().lastHeader(name) != null;
    }

    private static String hex(byte[] b) {
        return hex(b, b.length);
    }

    private static String hex(byte[] b, int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0) sb.append(' ');
            sb.append(String.format("%02x", b[i]));
        }
        return sb.toString();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
