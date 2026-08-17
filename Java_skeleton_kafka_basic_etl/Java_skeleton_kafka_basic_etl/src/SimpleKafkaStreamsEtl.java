import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

// =============================================================================
// SIMPLE KAFKA STREAMS ETL SKELETON
// source topic → validate → transform → enrich (KTable join) → output topic
//                        ↓
//                      DLQ topic
// =============================================================================
public class SimpleKafkaStreamsEtl {

    private static final String BOOTSTRAP_SERVERS    = "broker:9092";
    private static final String SOURCE_TOPIC         = "input-topic";
    private static final String OUTPUT_TOPIC         = "output-topic";
    private static final String DLQ_TOPIC            = "dlq-topic";
    private static final String REFERENCE_DATA_TOPIC = "reference-data-topic"; // KTable
    private static final String APP_ID               = "simple-etl-streams-app";


    // =========================================================================
    // MODEL
    // =========================================================================
    static class InputEvent {
        String id;
        String merchantId;
        double amount;

        InputEvent(String id, String merchantId, double amount) {
            this.id         = id;
            this.merchantId = merchantId;
            this.amount     = amount;
        }
    }

    static class ReferenceData {
        String merchantId;
        String merchantName;

        ReferenceData(String merchantId, String merchantName) {
            this.merchantId   = merchantId;
            this.merchantName = merchantName;
        }
    }

    static class OutputEvent {
        String id;
        String merchantId;
        String merchantName;
        double amount;

        OutputEvent(String id, String merchantId, String merchantName, double amount) {
            this.id           = id;
            this.merchantId   = merchantId;
            this.merchantName = merchantName;
            this.amount       = amount;
        }

        @Override
        public String toString() {
            return String.format("OutputEvent{id=%s merchantId=%s merchantName=%s amount=%.2f}",
                    id, merchantId, merchantName, amount);
        }
    }


    // =========================================================================
    // VALIDATOR
    // Returns null for invalid records — null values are filtered out
    // by the downstream filter() step and branched to the DLQ.
    // =========================================================================
    static class Validator {

        static boolean isValid(String key, InputEvent event) {
            if (key == null || key.isEmpty()) {
                System.err.println("[Validator] Null or empty key");
                return false;
            }
            if (event == null) {
                System.err.println("[Validator] Null event for key=" + key);
                return false;
            }
            if (event.amount < 0) {
                System.err.println("[Validator] Negative amount for key=" + key);
                return false;
            }
            // TODO: add further field validation
            return true;
        }
    }


    // =========================================================================
    // TRANSFORMER
    // Pure function — stateless mapping of InputEvent fields.
    // Applied before the KTable join so only valid, clean records are joined.
    // =========================================================================
    static class Transformer {

        // Returns a partially built OutputEvent — merchantName filled in by join
        static OutputEvent transform(String key, InputEvent event) {
            // TODO: add real transformation logic
            double adjustedAmount = event.amount * 1.0; // placeholder
            return new OutputEvent(event.id, event.merchantId, "unknown", adjustedAmount);
        }
    }


    // =========================================================================
    // TOPOLOGY BUILDER
    // Assembles the Kafka Streams processing graph.
    //
    // Key concepts used here:
    //
    // KStream  — unbounded stream of events, one record at a time
    // KTable   — changelog stream representing latest value per key.
    //            Used for reference data enrichment — always holds the
    //            most recent merchant profile without a full DB scan.
    //
    // KStream-KTable join:
    //   - Left side (KStream): incoming transaction events
    //   - Right side (KTable): merchant reference data
    //   - Join is key-based — merchantId must be the record key on both sides
    //   - Result: enriched OutputEvent with merchant name filled in
    //   - Staleness: join uses the KTable value at processing time.
    //     If reference data is updated after an event is processed,
    //     that event will not be retroactively re-enriched.
    //
    // branch() — splits a stream into multiple sub-streams by predicate.
    //   Used here to separate valid records from invalid ones (DLQ routing).
    // =========================================================================
    static Topology buildTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        // --- Step 1: Read source stream ---
        KStream<String, String> rawStream =
                builder.stream(SOURCE_TOPIC,
                        Consumed.with(Serdes.String(), Serdes.String()));

        // --- Step 2: Deserialize raw string to InputEvent ---
        KStream<String, InputEvent> parsedStream = rawStream.mapValues(
                value -> deserialize(value)
        );

        // --- Step 3: Branch into valid and invalid streams ---
        // branch() evaluates predicates in order — first match wins.
        // Valid records continue downstream; invalid records go to DLQ.
        Map<String, KStream<String, InputEvent>> branches = parsedStream.split(Named.as("branch-"))
                .branch((key, event) -> Validator.isValid(key, event),
                        Branched.as("valid"))
                .defaultBranch(Branched.as("invalid"));

        KStream<String, InputEvent> validStream   = branches.get("branch-valid");
        KStream<String, InputEvent> invalidStream = branches.get("branch-invalid");

        // --- Step 4: Route invalid records to DLQ ---
        // Serialize back to string with a simple error marker
        invalidStream
                .mapValues(event -> event == null ? "null-event" : "invalid:" + serialize(event))
                .to(DLQ_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // --- Step 5: Re-key by merchantId before KTable join ---
        // KStream-KTable join is key-based.
        // Input stream key may be transactionId — we re-key to merchantId
        // so it aligns with the KTable key (merchantId).
        // Note: re-keying causes a repartition (internal shuffle topic).
        KStream<String, InputEvent> rekeyedStream = validStream
                .selectKey((key, event) -> event.merchantId);

        // --- Step 6: Build reference data KTable ---
        // KTable always holds the latest value per key from the reference topic.
        // New merchant profile updates arrive as events on this topic —
        // the KTable materializes the current state automatically.
        KTable<String, String> referenceTable =
                builder.table(REFERENCE_DATA_TOPIC,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("reference-data-store")); // named state store

        // --- Step 7: Transform ---
        KStream<String, OutputEvent> transformedStream = rekeyedStream
                .mapValues((key, event) -> Transformer.transform(key, event));

        // --- Step 8: Enrich via KStream-KTable join ---
        // Left join: events without a matching KTable entry still flow through
        // (merchantName stays "unknown"). Use inner join if a match is required.
        KStream<String, OutputEvent> enrichedStream = transformedStream
                .leftJoin(
                        referenceTable,
                        (outputEvent, referenceValue) -> {
                            if (referenceValue != null) {
                                // Fill in merchant name from reference data
                                return new OutputEvent(
                                        outputEvent.id,
                                        outputEvent.merchantId,
                                        referenceValue, // TODO: deserialize to ReferenceData
                                        outputEvent.amount
                                );
                            }
                            // No match — return as-is with merchantName="unknown"
                            return outputEvent;
                        }
                );

        // --- Step 9: Produce to output topic ---
        enrichedStream
                .mapValues(event -> serialize(event))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }


    // =========================================================================
    // SERIALIZATION HELPERS
    // TODO: replace with Avro + schema registry in production.
    // KafkaAvroSerializer/Deserializer resolve schema from registry automatically
    // using the schema ID embedded in the Confluent wire format.
    // =========================================================================
    private static InputEvent deserialize(String value) {
        // TODO: implement JSON/Avro deserialization
        // Returning a stub for skeleton clarity
        if (value == null) return null;
        return new InputEvent("id-1", "merchant-1", 100.0);
    }

    private static String serialize(Object event) {
        // TODO: implement JSON/Avro serialization
        return event == null ? "" : event.toString();
    }


    // =========================================================================
    // MAIN — build topology, configure and start the streams app
    // =========================================================================
    public static void main(String[] args) {

        // --- Streams config ---
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());

        // Exactly-once processing guarantee.
        // Requires Kafka broker version 2.5+.
        // Cost: ~20% throughput reduction vs. at-least-once.
        // Remove this line and handle idempotency manually for at-least-once.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                StreamsConfig.EXACTLY_ONCE_V2);

        // State store location — RocksDB files stored here on disk.
        // Changelog topic backs this up for recovery after crash.
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/" + APP_ID);

        // --- Build topology ---
        Topology topology = buildTopology();
        System.out.println("[ETL] Topology:\n" + topology.describe());

        // --- Start streams app ---
        KafkaStreams streams = new KafkaStreams(topology, props);

        // --- Exception handler ---
        // Called when an unhandled exception occurs in the processing thread.
        // REPLACE: restart the streams instance (most common production choice).
        // FAIL_ON_ERROR: propagate and let the process crash + restart externally.
        streams.setUncaughtExceptionHandler(exception -> {
            System.err.println("[ETL] Uncaught exception: " + exception.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        // --- State change listener ---
        // Logs topology state transitions for observability.
        // RUNNING → REBALANCING means a partition rebalance is in progress.
        streams.setStateListener((newState, oldState) ->
                System.out.printf("[ETL] State transition: %s → %s%n", oldState, newState));

        // --- Graceful shutdown ---
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[ETL] Shutdown signal — closing streams...");
            streams.close(Duration.ofSeconds(10)); // wait up to 10s for clean shutdown
            System.out.println("[ETL] Shutdown complete");
        }));

        // --- Start ---
        streams.start();
        System.out.println("[ETL] Kafka Streams ETL started");
    }
}