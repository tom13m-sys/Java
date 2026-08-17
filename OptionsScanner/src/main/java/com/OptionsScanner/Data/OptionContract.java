package com.OptionsScanner.Data;

// =================================================================
// Shared contract POJO
// =================================================================
public class OptionContract {
    public final String symbol;
    public final double strike;
    public final int openInterest;
    public final long volume;
    public final double lastPrice;

    public OptionContract(String symbol, double strike, int openInterest, long volume, double lastPrice) {
        this.symbol = symbol;
        this.strike = strike;
        this.openInterest = openInterest;
        this.volume = volume;
        this.lastPrice = lastPrice;
    }
}