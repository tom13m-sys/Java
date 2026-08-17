import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import java.time.Duration;
import java.util.*;

// =============================================================================
// SIMPLE KAFKA ETL SKELETON
// consume → validate → transform → produce → commit
// =============================================================================
public class SimpleKafkaEtl {

    private static final String BOOTSTRAP_SERVERS = "broker:9092";
    private static final String SOURCE_TOPIC      = "input-topic";
    private static final String OUTPUT_TOPIC      = "output-topic";
    private static final String DLQ_TOPIC         = "dlq-topic";
    private static final String CONSUMER_GROUP    = "etl-consumer-group";


    // =========================================================================
    // MODEL
    // =========================================================================
    static class InputEvent {
        String id;
        String payload;

        InputEvent(String id, String payload) {
            this.id      = id;
            this.payload = payload;
        }
    }

    static class OutputEvent {
        String id;
        String processedPayload;

        OutputEvent(String id, String processedPayload) {
            this.id               = id;
            this.processedPayload = processedPayload;
        }
    }


    // =========================================================================
    // VALIDATOR
    // Returns empty if invalid — caller routes to DLQ
    // =========================================================================
    static class Validator {

        Optional<InputEvent> validate(String key, String value) {
            if (value == null || value.isEmpty()) {
                System.err.println("[Validator] Empty value for key=" + key);
                return Optional.empty();
            }
            // TODO: add schema / field validation
            return Optional.of(new InputEvent(key, value));
        }
    }


    // =========================================================================
    // TRANSFORMER
    // Pure function: InputEvent → OutputEvent
    // Keep stateless — easier to test and reason about
    // =========================================================================
    static class Transformer {

        OutputEvent transform(InputEvent event) {
            // TODO: replace with real transformation logic
            String processed = event.payload.toUpperCase();
            return new OutputEvent(event.id, processed);
        }
    }


    // =========================================================================
    // PRODUCER WRAPPER
    // Wraps KafkaProducer with a simple send method.
    // Async send with callback — logs success and failure.
    // =========================================================================
    static class EventProducer {

        private final KafkaProducer<String, String> producer;

        EventProducer() {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

            this.producer = new KafkaProducer<>(props);
        }

        void send(String topic, String key, String value) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("[Producer] Send failed for key=" + key
                            + " : " + exception.getMessage());
                } else {
                    System.out.println("[Producer] Sent key=" + key
                            + " to partition=" + metadata.partition()
                            + " offset=" + metadata.offset());
                }
            });
        }

        void close() {
            producer.flush(); // ensure all buffered records are sent before close
            producer.close();
        }
    }


    // =========================================================================
    // MAIN ETL LOOP
    // =========================================================================
    public static void main(String[] args) {

        // --- Build consumer ---
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // manual commit
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(SOURCE_TOPIC));

        // --- Build pipeline components ---
        Validator     validator   = new Validator();
        Transformer   transformer = new Transformer();
        EventProducer producer    = new EventProducer();

        // --- Graceful shutdown ---
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[ETL] Shutdown signal — closing...");
            consumer.wakeup();
        }));

        // --- Poll loop ---
        try {
            while (true) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1_000));

                for (ConsumerRecord<String, String> record : records) {

                    System.out.printf("[Consumer] Received key=%s partition=%d offset=%d%n",
                            record.key(), record.partition(), record.offset());

                    // Step 1: Validate
                    Optional<InputEvent> validated =
                            validator.validate(record.key(), record.value());

                    if (validated.isEmpty()) {
                        // Invalid — route to DLQ and continue
                        producer.send(DLQ_TOPIC, record.key(), record.value());
                        continue;
                    }

                    // Step 2: Transform
                    OutputEvent output = transformer.transform(validated.get());

                    // Step 3: Produce to output topic
                    producer.send(OUTPUT_TOPIC, output.id, output.processedPayload);
                }

                // Step 4: Commit offsets after batch is fully processed and produced
                // Manual commit — only reached if no exception was thrown above.
                // At-least-once: if crash occurs before commit, batch replays.
                if (!records.isEmpty()) {
                    consumer.commitSync();
                    System.out.println("[Consumer] Committed offsets for batch of "
                            + records.count() + " records");
                }
            }

        } catch (org.apache.kafka.common.errors.WakeupException e) {
            System.out.println("[ETL] Wakeup received — shutting down cleanly");

        } finally {
            consumer.commitSync(); // commit any remaining offsets before closing
            consumer.close();
            producer.close();
            System.out.println("[ETL] Shutdown complete");
        }
    }
}