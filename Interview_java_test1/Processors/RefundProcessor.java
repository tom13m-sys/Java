package Processors;

import Data.FinancialTracker;
import Data.RefundResult;
import Data.Base.EventBase;
import Data.Base.ResultBase;

public class RefundProcessor {
    private final FinancialTracker tracker;

    public RefundProcessor(FinancialTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    public ResultBase processEvent(EventBase event) {
        tracker.subtractRefund(event.getValue());
        return new RefundResult(tracker.getRunningTotal());
    }
}
