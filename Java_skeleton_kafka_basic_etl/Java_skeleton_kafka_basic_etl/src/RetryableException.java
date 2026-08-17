// =============================================================================
// EXCEPTION HIERARCHY
// Explicit classification of retryable vs. non-retryable failures.
// Each module must decide which category its failure belongs to.
// Non-retryable → DLQ immediately. Retryable → retry with backoff.
// =============================================================================
class RetryableException extends RuntimeException {
    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
