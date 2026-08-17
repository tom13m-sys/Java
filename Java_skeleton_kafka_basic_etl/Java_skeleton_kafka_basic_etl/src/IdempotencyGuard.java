import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


// =============================================================================
// IDEMPOTENCY GUARD
// Prevents duplicate processing when events are reprocessed after a
// consumer crash or rebalance (at-least-once delivery).
// Keyed on the business key (transactionId) — same key → safe no-op.
// =============================================================================
class IdempotencyGuard {

    // TODO: back this with Redis or a DB unique constraint in production.
    // In-memory set shown here for skeleton clarity only.
    private final Set<String> processedKeys = new HashSet<>();

    public boolean isAlreadyProcessed(String businessKey) {
        return processedKeys.contains(businessKey);
    }

    public void markProcessed(String businessKey) {
        processedKeys.add(businessKey);
    }
}
