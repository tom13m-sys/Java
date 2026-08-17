package Data.Base;

public abstract class ResultBase {

    private final double resultValue;
    
    protected ResultBase(double resultValue) {
        this.resultValue = resultValue;
    }

    public double getResultValue() {
        return resultValue;
    }
}
