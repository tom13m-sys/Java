package Validators;

import Data.LoginEvent;
import Data.Base.EventBase;
import Validators.Base.ValidatorBase;

public class LoginValidator extends ValidatorBase {

    public LoginValidator(LoginEvent event) {
        super(event);
    }

    @Override
    public boolean isEventValid() {
        // Enforce positive user IDs or login codes
        return event.getValue() > 0;
    }
    
}
