package Data;

// Shared mutable state context for financial calculations
public class FinancialTracker {
    private double runningTotal = 0.0;
    private int loginCount = 0;

    public void addPurchase(double amount) {
        this.runningTotal += amount;
    }

    public void subtractRefund(double amount) {
        this.runningTotal -= amount;
    }

    public void incrementLogins() {
        this.loginCount++;
    }

    public double getRunningTotal() {
        return runningTotal;
    }

    public int getLoginCount() {
        return loginCount;
    }
}
