package Validators;

import Data.RefundEvent;
import Data.Base.EventBase;
import Validators.Base.ValidatorBase;

public class RefundValidator extends ValidatorBase {

    public RefundValidator(RefundEvent event) {
        super(event);
    }

    @Override
    public boolean isEventValid() {
        // Refund amount details must be positive before processing subtraction
        return event.getValue() > 0;
    }
    
}
