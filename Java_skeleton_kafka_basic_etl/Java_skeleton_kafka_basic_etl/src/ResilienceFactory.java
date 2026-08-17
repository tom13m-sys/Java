// =============================================================================
// RESILIENCE FACTORY
// Builds shared Resilience4j instances.
// IMPORTANT: instances are shared across all threads — rate limiter and
// bulkhead enforce aggregate limits, not per-thread limits.
// Decorator order: circuit breaker → rate limiter → bulkhead → retry
// =============================================================================

import java.time.Duration;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ResilienceFactory {

    private static final Logger log = LoggerFactory.getLogger(ResilienceFactory.class);

    public static CircuitBreaker buildCircuitBreaker(String name) {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(PipelineConfig.CB_FAILURE_RATE_THRESHOLD)
                .slowCallDurationThreshold(
                    Duration.ofMillis(PipelineConfig.CB_SLOW_CALL_DURATION_MS))
                .waitDurationInOpenState(
                    Duration.ofMillis(PipelineConfig.CB_WAIT_DURATION_OPEN_MS))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(20)
                .build();

        CircuitBreaker cb = CircuitBreaker.of(name, config);

        // Log state transitions for observability
        cb.getEventPublisher()
          .onStateTransition(e -> log.warn(
              "[CircuitBreaker:{}] State transition: {}", name, e.getStateTransition()));

        return cb;
    }

    public static Retry buildRetry(String name) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(PipelineConfig.RETRY_MAX_ATTEMPTS)
                // Exponential backoff with jitter — prevents thundering herd
                // on a recovering downstream dependency
                .waitDuration(Duration.ofMillis(PipelineConfig.RETRY_INITIAL_BACKOFF_MS))
                .retryOnException(e -> e instanceof RetryableException)
                .ignoreExceptions(NonRetryableException.class) // go straight to DLQ
                .build();
        return Retry.of(name, config);
    }

    public static RateLimiter buildRateLimiter(String name) {
        // Shared across all threads — aggregate calls/sec across the process
        RateLimiterConfig config = RateLimiterConfig.custom()
                .limitForPeriod(PipelineConfig.RATE_LIMIT_CALLS_PER_SECOND)
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .timeoutDuration(Duration.ofMillis(500))
                .build();
        return RateLimiter.of(name, config);
    }

    public static Bulkhead buildBulkhead(String name) {
        // Limits concurrent calls to the external dependency across all threads
        BulkheadConfig config = BulkheadConfig.custom()
                .maxConcurrentCalls(PipelineConfig.BULKHEAD_MAX_CONCURRENT)
                .maxWaitDuration(Duration.ofMillis(100))
                .build();
        return Bulkhead.of(name, config);
    }
}
