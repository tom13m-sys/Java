package Validators;

import Data.PurchaseEvent;
import Data.Base.EventBase;
import Validators.Base.ValidatorBase;

public class PurchaseValidator extends ValidatorBase {

    public PurchaseValidator(PurchaseEvent event) {
        super(event);
    }

    @Override
    public boolean isEventValid() {
        // Financial purchases must be greater than zero
        return event.getValue() > 0;
    }
    
}
