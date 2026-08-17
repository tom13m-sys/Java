package Processors;

import Data.FinancialTracker;
import Data.LoginResult;
import Data.Base.EventBase;
import Data.Base.ResultBase;

public class LoginProcessor {
    private final FinancialTracker tracker;

    public LoginProcessor(FinancialTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    public ResultBase processEvent(EventBase event) {
        tracker.incrementLogins();
        return new LoginResult(tracker.getLoginCount());
    }
}
