package io.ibm.migration.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Consumer-side shim for Apicurio 2.x default payload framing after Cluster Linking replication.
 *
 * <h2>The problem</h2>
 * <p>When a producer uses the Apicurio 2.x serde with {@code ENABLE_HEADERS=false} and the default
 * {@code DefaultIdHandler}, it writes the schema ID as a 9-byte prefix:
 * <pre>
 *   byte[0]   = 0x00  (magic byte)
 *   byte[1..8] = schema ID as an 8-byte big-endian long
 *   byte[9..]  = raw Avro binary body
 * </pre>
 * Cluster Linking copies these bytes unchanged to the Confluent-side topic.  The Confluent
 * {@code KafkaAvroDeserializer} expects the standard 5-byte prefix:
 * <pre>
 *   byte[0]   = 0x00  (magic byte)
 *   byte[1..4] = schema ID as a 4-byte big-endian int
 *   byte[5..]  = raw Avro binary body
 * </pre>
 * Because the high 4 bytes of the 8-byte long are always zero for any real-world schema ID
 * (IDs start at 1 and increment; reaching 2^24 would require millions of registrations),
 * the Confluent deserializer reads bytes [1..4] = {@code 0x00 0x00 0x00 0x00} = ID 0, which
 * does not exist in any registry, and throws {@code Schema 0 not found}.
 *
 * <h2>The fix: OptimisticFallback detection</h2>
 * <p>This deserializer applies the same heuristic as Apicurio's own
 * {@code OptimisticFallbackIdHandler.readId()}:
 * <ol>
 *   <li>Read bytes [1..4] as a 4-byte big-endian int ({@code firstFour}).</li>
 *   <li>If {@code firstFour == 0} <em>and</em> there are at least 4 more bytes remaining,
 *       assume the record carries an 8-byte ID.  Reframe:
 *       {@code 0x00 + bytes[5..8] + bytes[9..]} → new 5-byte Confluent prefix + Avro body.</li>
 *   <li>Otherwise the record already has a 4-byte Confluent prefix; pass it through unchanged.</li>
 * </ol>
 *
 * <h2>Safety of the heuristic</h2>
 * <p>{@code firstFour == 0} in a real Confluent-format record would mean schema ID 0, which
 * no registry assigns (IDs start at 1).  The false-positive case therefore cannot occur in
 * practice.  The heuristic also works transparently on records that were already written with
 * the 4-byte Confluent format (e.g. by the 3.x serde or {@code Legacy4ByteIdHandler}):
 * those have a non-zero {@code firstFour} and are passed through untouched.
 *
 * <p>The heuristic fails only when an 8-byte schema ID has a non-zero value in its high 4 bytes,
 * i.e. ID ≥ 2^32 ≈ 4 billion.  That is not reachable in any existing deployment.
 *
 * <h2>Usage</h2>
 * <p>Configure this class as the value (or key) deserializer instead of
 * {@code KafkaAvroDeserializer}.  Pass all standard {@code schema.registry.*} properties
 * normally — they are forwarded to the inner {@code KafkaAvroDeserializer} unchanged.
 *
 * <pre>{@code
 * props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
 *     ApicurioPayloadDeserializer.class.getName());
 * props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
 *     "https://schema-registry.confluent.svc:8081");
 * }</pre>
 */
public final class ApicurioPayloadDeserializer implements Deserializer<Object> {

    private static final byte MAGIC = 0x00;

    private final KafkaAvroDeserializer inner = new KafkaAvroDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
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
     * Reframe an Apicurio 2.x 8-byte payload prefix to Confluent 4-byte prefix when detected.
     *
     * <p>Layout of an 8-byte-framed record (9-byte prefix total):
     * <pre>
     *   [0]      magic byte 0x00
     *   [1..4]   high 4 bytes of the 8-byte long — always 0x00000000 for IDs below 2^32
     *   [5..8]   low 4 bytes of the 8-byte long  — the actual schema ID as a 4-byte int
     *   [9..]    Avro binary body
     * </pre>
     *
     * <p>Layout of a 4-byte-framed record (5-byte prefix total, Confluent standard):
     * <pre>
     *   [0]      magic byte 0x00
     *   [1..4]   schema ID as a 4-byte big-endian int
     *   [5..]    Avro binary body
     * </pre>
     */
    public static byte[] reframe(byte[] data) {
        if (data == null || data.length < 5 || data[0] != MAGIC) {
            return data;
        }

        // Read bytes [1..4] as a big-endian int (the OptimisticFallbackIdHandler heuristic).
        int firstFour = ByteBuffer.wrap(data, 1, 4).getInt();

        if (firstFour == 0 && data.length >= 9) {
            // 8-byte framing detected.
            // bytes [5..8] hold the actual schema ID (low 4 bytes of the long).
            // Reframe: 0x00 + bytes[5..8] + bytes[9..] → standard 5-byte Confluent prefix + body.
            ByteBuffer out = ByteBuffer.allocate(data.length - 4);
            out.put(MAGIC);
            out.put(data, 5, 4);           // schema ID (low 4 bytes of the 8-byte long)
            out.put(data, 9, data.length - 9);  // Avro body unchanged
            return out.array();
        }

        // Already 4-byte Confluent format — pass through unchanged.
        return data;
    }
}
