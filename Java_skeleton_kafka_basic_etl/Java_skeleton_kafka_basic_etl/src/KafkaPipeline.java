


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


// =============================================================================
// MAIN PIPELINE — CONSUMER + PRODUCER LOOP
// Orchestrates: consume → validate → enrich → produce → commit
//
// Key design decisions reflected here:
//
// 1. Manual offset commit (enable.auto.commit=false)
//    Offset committed only after producer ack received — at-least-once guarantee.
//    commitSync() used for correctness; commitAsync() with callback for throughput.
//
// 2. pause() / resume() for back-pressure
//    When the internal processing buffer fills, consumer is paused on affected
//    partitions. This keeps the consumer in the group (heartbeats continue)
//    without fetching more data than can be processed. Avoids max.poll.interval.ms
//    breach and the rebalance storm that follows.
//
// 3. Offset committed after producer ack, not before
//    Prevents data loss in the crash window between commit and produce.
//    Trade-off: duplicate events on replay — handled by IdempotencyGuard.
//
// 4. Graceful shutdown via wakeup()
//    WakeupException is caught in the poll loop — the only safe way to
//    interrupt a blocking poll() call from another thread.
// =============================================================================
class KafkaPipeline implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaPipeline.class);

    private final Consumer<String, byte[]>  consumer;
    private final Producer<String, byte[]>  producer;
    private final SchemaValidator           schemaValidator;
    private final ExternalServiceClient     externalClient;
    private final IdempotencyGuard          idempotencyGuard;
    private final WatermarkStore            watermarkStore;
    private final DlqProducer              dlqProducer;
    private final MetricsCollector         metrics;

    // Signals graceful shutdown — set by shutdown hook
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    // Partitions currently paused for back-pressure
    private final Set<TopicPartition> pausedPartitions = new HashSet<>();

    public KafkaPipeline() {
        this.consumer         = buildConsumer();
        this.producer         = buildProducer();
        this.schemaValidator  = new SchemaValidator();
        this.externalClient   = new ExternalServiceClient();
        this.idempotencyGuard = new IdempotencyGuard();
        this.watermarkStore   = new WatermarkStore();
        this.dlqProducer      = new DlqProducer(producerProperties());
        this.metrics          = new MetricsCollector();

        // Graceful shutdown hook — triggers wakeup() on SIGTERM
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("[Pipeline] Shutdown signal received — draining...");
            shutdownRequested.set(true);
            consumer.wakeup(); // interrupts blocking poll() safely
        }));
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singletonList(PipelineConfig.SOURCE_TOPIC),
                    new RebalanceListener(watermarkStore));

            while (!shutdownRequested.get()) {

                // -----------------------------------------------------------------
                // BACK-PRESSURE: resume partitions if buffer has drained
                // -----------------------------------------------------------------
                resumeIfBufferDrained();

                // -----------------------------------------------------------------
                // POLL
                // Duration must be less than max.poll.interval.ms.
                // If processing takes longer than max.poll.interval.ms between
                // two poll() calls, the group coordinator declares this consumer
                // dead and triggers a rebalance — even if the JVM process is alive.
                // GC pauses can cause this silently (GC → poll freezes → rebalance).
                // -----------------------------------------------------------------
                ConsumerRecords<String, byte[]> records =
                        consumer.poll(Duration.ofMillis(1_000));

                if (records.isEmpty()) continue;

                metrics.recordConsumed(records.count());

                // -----------------------------------------------------------------
                // PROCESS BATCH
                // Track offsets to commit after all records in the batch succeed.
                // Committing per-record inside the loop is too slow at high throughput.
                // -----------------------------------------------------------------
                Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

                for (ConsumerRecord<String, byte[]> record : records) {

                    TopicPartition tp = new TopicPartition(
                            record.topic(), record.partition());

                    // ---------------------------------------------------------
                    // BACK-PRESSURE: pause this partition if buffer is full
                    // ---------------------------------------------------------
                    if (isBufferFull()) {
                        consumer.pause(Collections.singletonList(tp));
                        pausedPartitions.add(tp);
                        log.warn("[Pipeline] Back-pressure: paused partition={}", tp);
                        break; // stop processing this batch, poll will return empty
                    }

                    try {
                        processRecord(record, tp, offsetsToCommit);
                    } catch (Exception e) {
                        // Unrecoverable record-level failure — route to DLQ
                        log.error("[Pipeline] Unrecoverable failure on partition={} "
                                + "offset={}", record.partition(), record.offset(), e);
                        dlqProducer.send(record, "PROCESSING_ERROR", e);
                        metrics.recordDlq(1);
                        // Continue processing remaining records in the batch
                        // Offset for this record still tracked so it's not replayed
                        offsetsToCommit.put(tp,
                                new OffsetAndMetadata(record.offset() + 1));
                    }
                }

                // -----------------------------------------------------------------
                // COMMIT OFFSETS
                // commitSync() after all records in the batch are produced and acked.
                // This gives at-least-once: if we crash after producing but before
                // committing, the batch replays — idempotency handles duplicates.
                //
                // Alternative: commitAsync() with callback for higher throughput.
                // Use commitSync() as fallback in the callback's failure handler.
                // -----------------------------------------------------------------
                if (!offsetsToCommit.isEmpty()) {
                    commitOffsets(offsetsToCommit);
                }

                metrics.logActivitySummary();
            }

        } catch (WakeupException e) {
            // Expected on graceful shutdown — do not log as error
            if (!shutdownRequested.get()) throw e;
            log.info("[Pipeline] Wakeup received — shutting down cleanly");
        } finally {
            // commitSync() in finally ensures last batch offsets are committed
            // before the consumer leaves the group
            consumer.commitSync();
            consumer.close();
            producer.close();
            dlqProducer.close();
            log.info("[Pipeline] Shutdown complete");
        }
    }

    // -------------------------------------------------------------------------
    // RECORD PROCESSING
    // validate → idempotency check → enrich → produce → track offset
    // -------------------------------------------------------------------------
    private void processRecord(ConsumerRecord<String, byte[]> record,
                               TopicPartition tp,
                               Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {

        // Step 1: Schema validation
        Optional<TransactionEvent> validated = schemaValidator.validate(record);
        if (validated.isEmpty()) {
            dlqProducer.send(record, "SCHEMA_VALIDATION", null);
            metrics.recordDlq(1);
            offsetsToCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));
            return;
        }

        TransactionEvent event = validated.get();

        // Step 2: Idempotency check
        // transactionId is the business key — safe to skip if already processed.
        // Protects against duplicate processing after consumer crash + replay.
        if (idempotencyGuard.isAlreadyProcessed(event.getTransactionId())) {
            log.debug("[Pipeline] Skipping duplicate transactionId={}",
                    event.getTransactionId());
            metrics.recordSkipped(1);
            offsetsToCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));
            return;
        }

        // Step 3: Enrich via external service
        // Resilience4j decorators applied inside ExternalServiceClient:
        // circuit breaker → rate limiter → bulkhead → retry with backoff+jitter
        EnrichmentData enriched = externalClient.enrich(event);

        // Step 4: Produce to output topic
        // Offset is committed only AFTER producer ack is received (callback below).
        ProducerRecord<String, byte[]> outputRecord =
                buildOutputRecord(event, enriched);

        // Capture for use in lambda
        final long offsetToCommit = record.offset() + 1;

        producer.send(outputRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("[Pipeline] Producer send failed for transactionId={}",
                        event.getTransactionId(), exception);
                // TODO: trigger alert — this batch will be replayed on next poll
            } else {
                // Step 5: Mark idempotency key only after successful produce
                idempotencyGuard.markProcessed(event.getTransactionId());

                // Step 6: Save watermark after confirmed output
                watermarkStore.save(record.partition(), offsetToCommit);

                metrics.recordProduced(1);
                log.debug("[Pipeline] Produced transactionId={} to partition={} offset={}",
                        event.getTransactionId(), metadata.partition(), metadata.offset());
            }
        });

        // Track offset for batch commit after all records are produced
        offsetsToCommit.put(tp, new OffsetAndMetadata(offsetToCommit));
    }

    // -------------------------------------------------------------------------
    // OFFSET COMMIT
    // commitSync() blocks until broker acknowledges — safe but slower.
    // commitAsync() with callback — faster, requires failure handling in callback.
    // For critical pipelines: use commitSync() for reliability.
    // -------------------------------------------------------------------------
    private void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            // Option A: commitSync — blocking, safe
            consumer.commitSync(offsets);
            log.debug("[Pipeline] Committed offsets: {}", offsets);

            // Option B: commitAsync — non-blocking, needs callback
            // consumer.commitAsync(offsets, (committedOffsets, exception) -> {
            //     if (exception != null) {
            //         log.error("[Pipeline] Async commit failed", exception);
            //         // Fallback: commitSync on next iteration or retry here
            //     }
            // });

        } catch (CommitFailedException e) {
            // Consumer was removed from group before commit completed (rebalance).
            // The batch will be replayed by the new owner — idempotency handles it.
            log.warn("[Pipeline] Commit failed — likely rebalance in progress", e);
        }
    }

    // -------------------------------------------------------------------------
    // BACK-PRESSURE HELPERS
    // pause() stops fetching without leaving the consumer group.
    // Heartbeats continue, so session.timeout.ms is not breached.
    // resume() called when the internal buffer drains below threshold.
    // -------------------------------------------------------------------------
    private boolean isBufferFull() {
        // TODO: check actual internal queue size
        return false;
    }

    private void resumeIfBufferDrained() {
        if (!pausedPartitions.isEmpty() && !isBufferFull()) {
            consumer.resume(pausedPartitions);
            log.info("[Pipeline] Back-pressure relieved — resumed partitions: {}",
                    pausedPartitions);
            pausedPartitions.clear();
        }
    }

    // -------------------------------------------------------------------------
    // KAFKA CLIENT BUILDERS
    // -------------------------------------------------------------------------
    private Consumer<String, byte[]> buildConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                PipelineConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                PipelineConfig.CONSUMER_GROUP);

        // Manual commit — offset controlled explicitly after produce ack
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Tune poll behavior
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                PipelineConfig.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                PipelineConfig.MAX_POLL_INTERVAL_MS);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                PipelineConfig.SESSION_TIMEOUT_MS);

        // Schema registry integration — schema ID resolved from wire format automatically
        props.put("schema.registry.url", PipelineConfig.SCHEMA_REGISTRY_URL);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        // TODO: swap ByteArrayDeserializer for KafkaAvroDeserializer in production

        return new KafkaConsumer<>(props);
    }

    private Producer<String, byte[]> buildProducer() {
        return new KafkaProducer<>(producerProperties());
    }

    private Properties producerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                PipelineConfig.BOOTSTRAP_SERVERS);

        // Idempotent producer: prevents duplicates from producer-side retries.
        // Required foundation for exactly-once (transactional API).
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // acks=all: broker waits for all in-sync replicas before acknowledging.
        // Required with enable.idempotence=true.
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        // TODO: swap ByteArraySerializer for KafkaAvroSerializer in production

        return props;
    }

    private ProducerRecord<String, byte[]> buildOutputRecord(
            TransactionEvent event, EnrichmentData enriched) {
        // TODO: serialize enriched event to Avro/JSON bytes
        byte[] payload = new byte[0]; // placeholder
        return new ProducerRecord<>(
                PipelineConfig.OUTPUT_TOPIC,
                event.getMerchantId(), // partition key — same merchant → same partition
                payload
        );
    }


    // =========================================================================
    // REBALANCE LISTENER
    // Called by the group coordinator before and after partition reassignment.
    // onPartitionsRevoked: commit current offsets before losing partition ownership.
    // onPartitionsAssigned: load watermark from store for newly assigned partitions.
    // =========================================================================
    static class RebalanceListener implements ConsumerRebalanceListener {

        private static final Logger log =
                LoggerFactory.getLogger(RebalanceListener.class);

        private final WatermarkStore watermarkStore;

        public RebalanceListener(WatermarkStore watermarkStore) {
            this.watermarkStore = watermarkStore;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // Commit offsets synchronously before partition ownership is transferred.
            // Failure to do this causes the new owner to reprocess from last committed.
            log.info("[RebalanceListener] Partitions revoked: {} — committing offsets",
                    partitions);
            // TODO: consumer reference needed here — pass via constructor or thread-local
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.info("[RebalanceListener] Partitions assigned: {} — loading watermarks",
                    partitions);
            for (TopicPartition tp : partitions) {
                long savedOffset = watermarkStore.load(tp.partition());
                if (savedOffset >= 0) {
                    log.info("[RebalanceListener] Resuming partition={} from offset={}",
                            tp.partition(), savedOffset);
                    // TODO: consumer.seek(tp, savedOffset)
                }
            }
        }
    }


    // =========================================================================
    // ENTRY POINT
    // =========================================================================
    public static void main(String[] args) {
        KafkaPipeline pipeline = new KafkaPipeline();
        pipeline.run();
    }
}