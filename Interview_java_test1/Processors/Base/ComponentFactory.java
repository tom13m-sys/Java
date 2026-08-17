package Processors.Base;

import Data.FinancialTracker;
import Data.Base.EventBase;
import Validators.Base.ValidatorBase;

// This generic interface strictly binds an Event type to its handling components
public interface ComponentFactory<E extends EventBase, V extends ValidatorBase, P extends ProcessorBase> {
    V createValidator(E event);
    P createProcessor(FinancialTracker tracker);
}