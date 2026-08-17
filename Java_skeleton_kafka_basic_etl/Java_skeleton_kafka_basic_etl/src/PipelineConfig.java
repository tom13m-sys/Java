
// =============================================================================
// CONFIGURATION
// Centralizes all tunable parameters. Loaded from an external config file
// in production — swap with your config framework of choice.
// =============================================================================
class PipelineConfig {

    // Kafka
    public static final String BOOTSTRAP_SERVERS   = "broker:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://schema-registry:8081";
    public static final String SOURCE_TOPIC        = "merchant-transactions";
    public static final String OUTPUT_TOPIC        = "merchant-aggregates";
    public static final String DLQ_TOPIC           = "merchant-transactions-dlq";
    public static final String CONSUMER_GROUP      = "enrichment-service-group";

    // Consumer tuning
    // max.poll.interval.ms must exceed the longest possible processing time
    // per batch — otherwise the group coordinator will declare the consumer
    // dead and trigger a rebalance (GC pause → poll freeze → rebalance storm)
    public static final int MAX_POLL_RECORDS       = 500;
    public static final int MAX_POLL_INTERVAL_MS   = 300_000; // 5 minutes
    public static final int SESSION_TIMEOUT_MS     = 45_000;

    // Back-pressure: pause consumer when internal queue exceeds this size
    public static final int BACK_PRESSURE_THRESHOLD = 1_000;

    // Resilience4j
    public static final int  CB_FAILURE_RATE_THRESHOLD   = 50;   // % failures to open CB
    public static final int  CB_SLOW_CALL_DURATION_MS    = 2_000;
    public static final int  CB_WAIT_DURATION_OPEN_MS    = 10_000;
    public static final int  RETRY_MAX_ATTEMPTS          = 3;
    public static final long RETRY_INITIAL_BACKOFF_MS    = 500;
    public static final int  RATE_LIMIT_CALLS_PER_SECOND = 100;
    public static final int  BULKHEAD_MAX_CONCURRENT     = 20;   // shared across all threads
}

