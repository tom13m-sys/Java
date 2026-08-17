// =============================================================================
// WATERMARK STORE
// Persists the last successfully processed timestamp/offset so that on
// restart the pipeline resumes from exactly the right position.
// The watermark is committed AFTER producer acks are received —
// never before — to prevent silent data loss.
// =============================================================================

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WatermarkStore {

    private static final Logger log = LoggerFactory.getLogger(WatermarkStore.class);

    // TODO: replace with Redis, ZooKeeper, or a dedicated Kafka watermark topic
    // (writing watermark to a Kafka topic inside the same producer transaction
    //  gives true atomicity between data commit and watermark commit)
    private final Map<Integer, Long> partitionOffsets = new HashMap<>();

    public void save(int partition, long offset) {
        partitionOffsets.put(partition, offset);
        log.info("[WatermarkStore] Saved offset={} for partition={}", offset, partition);
        // TODO: persist to external store
    }

    public long load(int partition) {
        return partitionOffsets.getOrDefault(partition, -1L);
    }
}
