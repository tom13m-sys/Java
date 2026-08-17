package Data;

import Data.Base.EventBase;

public abstract class RefundEvent extends EventBase {

    public RefundEvent(double value) {
        super(value);
    }
}
