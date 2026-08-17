package Data.Base;

import java.util.ArrayList;

public abstract class EventBase {

    private final double value;

    protected EventBase(double value) {
        this.value = value;
    }
    public double getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.format("Event Type: %s, Value: %.2f", this.getClass().getSimpleName(), value);
    }

}