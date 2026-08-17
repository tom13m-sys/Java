// =============================================================================
// DLQ PRODUCER
// Routes failed records to the dead letter queue with classification metadata.
// Keeps the main pipeline moving — failed records are handled asynchronously.
// =============================================================================
class DlqProducer {

    private static final Logger log = LoggerFactory.getLogger(DlqProducer.class);

    private final Producer<String, byte[]> producer;

    public DlqProducer(Properties producerProps) {
        this.producer = new KafkaProducer<>(producerProps);
    }

    public void send(ConsumerRecord<String, byte[]> originalRecord,
                     String failureClassification,
                     Exception cause) {

        // Attach failure classification as a header so ops can filter DLQ by type:
        // SCHEMA_VALIDATION | PROCESSING_ERROR | EXTERNAL_SERVICE_FAILURE
        ProducerRecord<String, byte[]> dlqRecord =
                new ProducerRecord<>(PipelineConfig.DLQ_TOPIC,
                        originalRecord.partition(),
                        originalRecord.key(),
                        originalRecord.value());

        dlqRecord.headers()
                 .add("failure-classification", failureClassification.getBytes())
                 .add("original-topic", PipelineConfig.SOURCE_TOPIC.getBytes())
                 .add("original-partition",
                         String.valueOf(originalRecord.partition()).getBytes())
                 .add("original-offset",
                         String.valueOf(originalRecord.offset()).getBytes());

        producer.send(dlqRecord, (metadata, exception) -> {
            if (exception != null) {
                // DLQ send failure — escalate immediately, do not swallow
                log.error("[DlqProducer] CRITICAL: failed to write to DLQ for key={}",
                        originalRecord.key(), exception);
                // TODO: trigger PagerDuty / OpsGenie alert
            } else {
                log.warn("[DlqProducer] Routed failed record key={} classification={} "
                        + "to DLQ partition={} offset={}",
                        originalRecord.key(), failureClassification,
                        metadata.partition(), metadata.offset());
            }
        });
    }

    public void close() { producer.close(); }
}
