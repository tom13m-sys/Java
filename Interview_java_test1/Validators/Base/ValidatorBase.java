package Validators.Base;

import Data.Base.EventBase;

public abstract class ValidatorBase {

    protected final EventBase event;

    protected ValidatorBase(EventBase event) {
        this.event = event;
    }
    
    public abstract boolean isEventValid();
}
