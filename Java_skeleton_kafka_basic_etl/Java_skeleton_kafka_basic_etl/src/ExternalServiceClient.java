import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



// =============================================================================
// EXTERNAL SERVICE CLIENT
// Wraps all calls to external dependencies (APIs, DBs, metadata stores).
// Resilience4j decorators applied in correct order:
//   circuit breaker → rate limiter → bulkhead → retry
// =============================================================================
class ExternalServiceClient {

    private static final Logger log = LoggerFactory.getLogger(ExternalServiceClient.class);

    private final CircuitBreaker circuitBreaker;
    private final RateLimiter    rateLimiter;
    private final Bulkhead       bulkhead;
    private final Retry          retry;

    public ExternalServiceClient() {
        // Shared instances — aggregate limits across all threads in the process
        this.circuitBreaker = ResilienceFactory.buildCircuitBreaker("external-service");
        this.rateLimiter    = ResilienceFactory.buildRateLimiter("external-service");
        this.bulkhead       = ResilienceFactory.buildBulkhead("external-service");
        this.retry          = ResilienceFactory.buildRetry("external-service");
    }

    public EnrichmentData enrich(TransactionEvent event) {
        // Decorators applied in order: CB outermost → rate limiter → bulkhead → retry
        // CB must be outermost so it sees all failures including retry exhaustion
        return CircuitBreaker.decorateSupplier(circuitBreaker,
               RateLimiter.decorateSupplier(rateLimiter,
               Bulkhead.decorateSupplier(bulkhead,
               Retry.decorateSupplier(retry,
               () -> callExternalService(event)
               )))).get();
    }

    private EnrichmentData callExternalService(TransactionEvent event) {
        try {
            // TODO: implement actual HTTP/DB call
            // Classify the exception before throwing:
            //   HTTP 429 / 503 / timeout → RetryableException
            //   HTTP 400 / schema error  → NonRetryableException (goes to DLQ)
            return new EnrichmentData(event.getMerchantId());
        } catch (Exception e) {
            if (isRetryable(e)) throw new RetryableException("Transient failure", e);
            throw new NonRetryableException("Permanent failure", e);
        }
    }

    private boolean isRetryable(Exception e) {
        // TODO: inspect HTTP status code or exception type
        return true;
    }
}
