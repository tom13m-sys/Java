// =============================================================================
// EVENT MODEL
// Typed representation of an incoming Kafka event.
// Schema validation is applied before this object is constructed —
// invalid events never enter the processing pipeline.
// =============================================================================
class TransactionEvent {
    private final String merchantId;
    private final String transactionId; // business key — used for idempotency
    private final double amount;
    private final long   eventTimestampMs;

    public TransactionEvent(String merchantId, String transactionId,
                            double amount, long eventTimestampMs) {
        this.merchantId       = merchantId;
        this.transactionId    = transactionId;
        this.amount           = amount;
        this.eventTimestampMs = eventTimestampMs;
    }

    public String getMerchantId()       { return merchantId; }
    public String getTransactionId()    { return transactionId; }
    public double getAmount()           { return amount; }
    public long   getEventTimestampMs() { return eventTimestampMs; }
}
