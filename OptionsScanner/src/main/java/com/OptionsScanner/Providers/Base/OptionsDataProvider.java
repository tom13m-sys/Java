package com.OptionsScanner.Providers.Base;

import java.time.LocalDate;
import java.util.List;

import com.OptionsScanner.Data.*;

// =================================================================
// Provider abstraction — swap implementations without touching
// the scanning logic above.
// =================================================================
public interface OptionsDataProvider {
    String name();

    double getLastClose(String ticker) throws Exception;

    List<OptionContract> getCallChain(String ticker, LocalDate expiration, double minStrike, double maxStrike) throws Exception;

    /**
     * Raw trailing daily volume series for a single option contract
     * (most-recent-first not required; order doesn't matter to the
     * caller). Used to derive the dynamic per-contract floor and the
     * median/MAD baseline for Phase 2 spike detection. Returns an
     * empty list if no historical data is available.
     */
    List<Long> getHistoricalVolumes(String optionSymbol, int lookbackDays) throws Exception;

    double getPreviousClose(String optionSymbol) throws Exception;
    
    /** Provider-specific throttle to stay within its free-tier rate limit. */
    default void pause() {
        try {
            Thread.sleep(getRateLimitPauseMs());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    long getRateLimitPauseMs();
}