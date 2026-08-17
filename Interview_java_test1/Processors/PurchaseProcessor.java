package Processors;

import Data.FinancialTracker;
import Data.PurchaseResult;
import Data.Base.EventBase;
import Data.Base.ResultBase;

public class PurchaseProcessor {
    private final FinancialTracker tracker;

    public PurchaseProcessor(FinancialTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    public ResultBase processEvent(EventBase event) {
        tracker.addPurchase(event.getValue());
        return new PurchaseResult(tracker.getRunningTotal());
    }
}
