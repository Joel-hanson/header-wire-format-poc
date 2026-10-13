package io.ibm.migration;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public final class OrderSchema {
    public static final String TOPIC = "orders";
    public static final String SUBJECT = "orders-value";

    private static final Schema SCHEMA = load();

    /** Backward-compatible evolution of v1: adds an optional field, so it gets a new schema ID. */
    private static final Schema SCHEMA_V2 = new Schema.Parser().parse("""
            {"type":"record","name":"Order","namespace":"io.ibm.migration",
             "fields":[{"name":"orderId","type":"string"},
                       {"name":"customer","type":"string"},
                       {"name":"amount","type":"double"},
                       {"name":"currency","type":"string","default":"USD"}]}
            """);

    private OrderSchema() {
    }

    public static Schema schema() {
        return SCHEMA;
    }

    public static GenericRecord order(String orderId, String customer, double amount) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("orderId", orderId);
        record.put("customer", customer);
        record.put("amount", amount);
        return record;
    }

    public static GenericRecord orderV2(String orderId, String customer, double amount, String currency) {
        GenericRecord record = new GenericData.Record(SCHEMA_V2);
        record.put("orderId", orderId);
        record.put("customer", customer);
        record.put("amount", amount);
        record.put("currency", currency);
        return record;
    }

    private static Schema load() {
        try (InputStream in = OrderSchema.class.getClassLoader().getResourceAsStream("order.avsc")) {
            if (in == null) {
                // Fallback inline schema so shade jar still works without resource copy.
                return new Schema.Parser().parse("""
                        {"type":"record","name":"Order","namespace":"io.ibm.migration",
                         "fields":[{"name":"orderId","type":"string"},
                                   {"name":"customer","type":"string"},
                                   {"name":"amount","type":"double"}]}
                        """);
            }
            return new Schema.Parser().parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load order.avsc", e);
        }
    }
}
