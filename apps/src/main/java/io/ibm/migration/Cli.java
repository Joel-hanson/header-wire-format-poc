package io.ibm.migration;

import io.ibm.migration.consumer.ApicurioHeaderConsumer;
import io.ibm.migration.consumer.ConfluentShimConsumer;
import io.ibm.migration.consumer.ConfluentWireConsumer;
import io.ibm.migration.producer.ApicurioHeaderProducer;
import io.ibm.migration.verify.PayloadIdVerifier;

/**
 * Usage:
 *   java -jar apps.jar produce         [count]
 *   java -jar apps.jar produce-v2      [count]
 *   java -jar apps.jar consume-es
 *   java -jar apps.jar consume-cflt
 *   java -jar apps.jar consume-cflt-shim
 *   java -jar apps.jar verify-ids
 */
public final class Cli {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            usage();
            System.exit(1);
        }
        switch (args[0]) {
            case "produce" -> {
                int count = args.length > 1 ? Integer.parseInt(args[1]) : 5;
                ApicurioHeaderProducer.run(count);
            }
            case "produce-v2" -> {
                int count = args.length > 1 ? Integer.parseInt(args[1]) : 3;
                ApicurioHeaderProducer.run(count, true);
            }
            case "consume-es"        -> ApicurioHeaderConsumer.run();
            case "consume-cflt"      -> ConfluentWireConsumer.run();
            case "consume-cflt-shim" -> ConfluentShimConsumer.run();
            case "verify-ids"        -> System.exit(PayloadIdVerifier.run());
            default -> {
                usage();
                System.exit(1);
            }
        }
    }

    private static void usage() {
        System.err.println("""
                Usage:
                  produce [count]      Produce Apicurio-serde Avro to source (ES stand-in)
                  produce-v2 [count]   Same, with the evolved v2 schema (new schema ID)
                  consume-es           Consume with Apicurio header-aware deserializer
                  consume-cflt         Consume rewritten topic with Confluent deserializer
                  consume-cflt-shim    Consume 8-byte-payload topic via shim deserializer
                  verify-ids           Per-record check: Confluent payload ID == Apicurio header ID
                """);
    }
}
