// =============================================================================
// METRICS COLLECTOR
// Exposes pipeline health metrics for Grafana/Prometheus dashboards.
// Baseline is established at startup; drift from baseline triggers alerts.
// =============================================================================
class MetricsCollector {

    // TODO: replace with Micrometer / Prometheus registry
    // Key metrics to expose:
    //   - consumer lag per partition
    //   - records consumed / produced rate
    //   - processing time per record (avg, p99)
    //   - DLQ record count (by classification)
    //   - circuit breaker state
    //   - external service call latency
    //   - JVM GC pause time and frequency  ← connects to max.poll.interval.ms risk
    //   - heap usage / thread pool saturation

    private long totalConsumed  = 0;
    private long totalProduced  = 0;
    private long totalDlq       = 0;
    private long totalSkipped   = 0; // idempotency hits

    public void recordConsumed(int count)  { totalConsumed  += count; }
    public void recordProduced(int count)  { totalProduced  += count; }
    public void recordDlq(int count)       { totalDlq       += count; }
    public void recordSkipped(int count)   { totalSkipped   += count; }

    public void logActivitySummary() {
        // TODO: push to Prometheus / structured log
        System.out.printf("[Metrics] consumed=%d produced=%d dlq=%d skipped=%d%n",
                totalConsumed, totalProduced, totalDlq, totalSkipped);
    }
}
