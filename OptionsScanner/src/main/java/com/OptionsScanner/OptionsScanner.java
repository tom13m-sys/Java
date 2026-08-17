package com.OptionsScanner;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.OptionsScanner.Data.*;
import com.OptionsScanner.Providers.Base.OptionsDataProvider;
import com.OptionsScanner.Providers.*;

/**
 * PHASE 1 + PHASE 2 DEMO — Options Chain Scanner (provider-agnostic)
 * ---------------------------------------------------------------
 * PHASE 1 — for a selected equity (default: GLD):
 *   1. Scans all future monthly OPEX dates (3rd Friday of each month).
 *   2. For each OPEX, pulls the call-option chain and filters to strikes
 *      greater than the underlying's last closing price.
 *   3. Prints the top 10 contracts (by Open Interest) for each OPEX,
 *      sorted descending.
 *
 * PHASE 2 — for each OPEX's top 10 contracts from Phase 1:
 *   4. Pulls each contract's trailing volume history and derives a
 *      DYNAMIC per-contract floor from the max of: its own 90th
 *      percentile volume, a fraction of its open interest, and a small
 *      hard minimum (handles thin/new contracts with little history).
 *   5. Flags a contract only if today's volume clears that floor AND is
 *      a robust statistical outlier vs. its trailing median (median +
 *      MAD z-score > 3.5), which is far less noise-prone than a flat
 *      multiplier against a mean when the baseline is near-zero.
 *   6. No follow-up inspection logic is implemented yet — flagged
 *      contracts are only collected and printed in a summary
 *      (Phase 3 will act on this list).
 *
 * DATA PROVIDER — SWITCHABLE
 * ---------------------------------------------------------------
 * Two providers are implemented behind a common OptionsDataProvider
 * interface:
 *   - MarketDataProvider (DEFAULT) — MarketData.app "Free Forever" plan
 *     grants delayed options chain snapshot data (incl. Open Interest)
 *     with no credit card, 100 requests/day.
 *   - PolygonProvider — Polygon.io. NOTE: options snapshot/OI data is
 *     gated behind a paid Options plan even on otherwise-active free
 *     API keys; this path will throw a clear 403 error if your plan
 *     doesn't include it.
 *
 * Switch providers with an environment variable — no code changes needed:
 *   export DATA_PROVIDER=MARKETDATA   (default)
 *   export DATA_PROVIDER=POLYGON
 *
 * Required API keys (only the active provider's key is needed):
 *   export MARKETDATA_API_KEY=your_key_here
 *   export POLYGON_API_KEY=your_key_here
 *
 * Requires org.json on the classpath (json-20240303.jar or later):
 *   https://mvnrepository.com/artifact/org.json/json
 *
 * Compile:  javac -cp json-20240303.jar OptionsScanner.java
 * Run:      java  -cp .:json-20240303.jar OptionsScanner
 * 
 * 
 * TODO: Use OI and volume to calculate notional value. High Notional value can be a string indicator (Maybe compare to market cap)
 * TODO: Volume spike needs to be searched intraday
 * 
 * 
 */
public class OptionsScanner {

    // ---- Configuration -------------------------------------------------
    // private static final String UNDERLYING = "SNDK";
    //private static final String[] UNDERLYING_LIST = {"ETHA", "TSLA", "NVDA", "CRCL", "GLD", "IBIT", "COIN"};
    private static final String[] UNDERLYING_LIST = {"ETHA", "TSLA"};
    private static final int MONTHS_AHEAD = 6;   // how many future monthly OPEX cycles to scan
    private static final int START_MONTHS = 1;   // how many future monthly OPEX cycles to initially skip
    private static final int TOP_N = 10;          // top contracts to print per OPEX (per spec)

    // Phase 1: strike range filter — 
    // strike must be at least 10% higher than last close
    // strike must be greater than last close
    // but no more than STRIKE_UPPER_MULTIPLIER x last close (150% per spec)
    private static final double STRIKE_LOWER_MULTIPLIER = 1.1;
    private static final double STRIKE_UPPER_MULTIPLIER = 1.5;

    // Phase 2: volume-spike detection
    private static final int HISTORY_LOOKBACK_DAYS = 20;    // trailing days pulled for baseline stats
    private static final int MIN_HISTORY_DAYS = 10;         // below this, skip the percentile floor (too new/thin)
    private static final double FLOOR_PERCENTILE = 90.0;    // percentile of own history used as liquidity floor
    private static final double OI_FLOOR_FRACTION = 0.03;   // fallback floor: 3% of open interest
    private static final double HARD_MIN_FLOOR = 10.0;      // absolute noise gate regardless of the above
    private static final double MAD_Z_CONST = 0.6745;       // standard MAD->z scaling constant
    private static final double MAD_MIN = 1.0;              // avoid div-by-zero when trailing days are all near-identical
    private static final double Z_SCORE_THRESHOLD = 3.5;    // Iglewicz-Hoaglin robust outlier threshold

    public static void main(String[] args) throws Exception {
        // String ticker = args.length > 0 ? args[0].toUpperCase() : UNDERLYING;

        OptionsDataProvider provider = buildProvider();
        System.out.println("Provider:   " + provider.name());

        List<OptionContract> flaggedForFollowUp = new ArrayList<>();
        for (String ticker : UNDERLYING_LIST) {
            
            System.out.println("=== Options Scanner (Phase 1 Demo) ===");
            System.out.println("Underlying: " + ticker);

            // Step 0: last closing price of the underlying
            double lastClose = provider.getLastClose(ticker);
            System.out.printf("Last closing price for %s: $%.2f%n%n", ticker, lastClose);
            provider.pause();

            // Step 1: future monthly OPEX dates
            List<LocalDate> opexDates = getFutureMonthlyOpexDates(START_MONTHS, MONTHS_AHEAD);
            System.out.println("Scanning " + opexDates.size() + " future monthly OPEX dates: " + opexDates);
            System.out.println();

            // Step 2 + 3: for each OPEX, fetch call chain, filter, rank, print.
            // Step 4-6 (Phase 2): scan that OPEX's top 10 for volume spikes,
            // accumulating any flagged contracts into a running list.
            for (LocalDate opex : opexDates) {
                try {
                    scanOpexDate(provider, ticker, opex, lastClose, flaggedForFollowUp);
                } catch (Exception e) {
                    System.err.println("Failed to scan OPEX " + opex + ": " + e.getMessage());
                }
                provider.pause();
            }
        }

        printFollowUpSummary(flaggedForFollowUp);

        System.out.println("=== Done ===");
    }

    // ---------------------------------------------------------------
    // Phase 2 summary: lists every contract flagged for follow-up.
    // Follow-up inspection itself is intentionally NOT implemented —
    // that's Phase 3.
    // ---------------------------------------------------------------
    private static void printFollowUpSummary(List<OptionContract> flagged) {
        System.out.println("=== Phase 2 Summary: Contracts Flagged For Follow-Up ===");
        if (flagged.isEmpty()) {
            System.out.println("  None flagged.");
        } else {
            for (OptionContract c : flagged) {
                System.out.printf("  %-26s strike=%.2f OI=%d volume=%d%n",
                        c.symbol, c.strike, c.openInterest, c.volume);
            }
        }
        System.out.println("  (Follow-up inspection is not implemented yet — Phase 3.)");
        System.out.println();
    }

    // ---------------------------------------------------------------
    // Selects the active provider from the DATA_PROVIDER env var.
    // Defaults to MARKETDATA if unset.
    // ---------------------------------------------------------------
    private static OptionsDataProvider buildProvider() {
        String choice = System.getenv().getOrDefault("DATA_PROVIDER", "MARKETDATA").trim().toUpperCase();
        switch (choice) {
            case "POLYGON":
                return new PolygonProvider();
            case "MARKETDATA":
                return new MarketDataProvider();
            default:
                throw new IllegalArgumentException(
                        "Unknown DATA_PROVIDER '" + choice + "'. Use MARKETDATA or POLYGON.");
        }
    }

    // ---------------------------------------------------------------
    // Computes the 3rd Friday (standard monthly OPEX) of each of the
    // next `monthsAhead` months, starting from the current month,
    // excluding any date that has already passed.
    // ---------------------------------------------------------------
    private static List<LocalDate> getFutureMonthlyOpexDates(int startMonths, int monthsAhead) {
        List<LocalDate> dates = new ArrayList<>();
        LocalDate today = LocalDate.now().plusMonths(startMonths);
        LocalDate cursor = today.withDayOfMonth(1);

        for (int i = 0; i <= monthsAhead; i++) {
            LocalDate opex = thirdFriday(cursor);
            if (!opex.isBefore(today)) {
                dates.add(opex);
            }
            cursor = cursor.plusMonths(1);
        }
        return dates;
    }

    private static LocalDate thirdFriday(LocalDate anyDayInMonth) {
        LocalDate firstOfMonth = anyDayInMonth.withDayOfMonth(1);
        int daysUntilFriday = (DayOfWeek.FRIDAY.getValue() - firstOfMonth.getDayOfWeek().getValue() + 7) % 7;
        LocalDate firstFriday = firstOfMonth.plusDays(daysUntilFriday);
        return firstFriday.plusWeeks(2); // 3rd Friday
    }

    // ---------------------------------------------------------------
    // Fetches the call chain for one OPEX date from the active
    // provider, filters to lastClose < strike < 1.5x lastClose,
    // sorts by OI desc, and prints the top N.
    // ---------------------------------------------------------------
    private static void scanOpexDate(OptionsDataProvider provider, String ticker,
                                      LocalDate opex, double lastClose,
                                      List<OptionContract> flaggedForFollowUp) throws Exception {
        String expStr = opex.format(DateTimeFormatter.ISO_LOCAL_DATE);
        System.out.println("--- OPEX: " + expStr + " ---");

        double maxStrike = lastClose * STRIKE_UPPER_MULTIPLIER;
        double minStrike = lastClose * STRIKE_LOWER_MULTIPLIER;
        List<OptionContract> contracts = provider.getCallChain(ticker, opex, minStrike, maxStrike);

        if (contracts.isEmpty()) {
            System.out.printf("  No call contracts found in strike range ($%.2f, $%.2f)%n", minStrike, maxStrike);
            System.out.println();
            return;
        }

        contracts.sort((a, b) -> Integer.compare(b.openInterest, a.openInterest));

        System.out.printf("  Found %d call contracts with $%.2f < strike < $%.2f. Top %d by Open Interest:%n",
                contracts.size(), minStrike, maxStrike, TOP_N);
        System.out.printf("  %-26s %10s %10s %10s %10s%n", "Contract", "Strike", "OI", "Volume", "Last");

        List<OptionContract> top = new ArrayList<>();
        for (OptionContract c : contracts) {
            if (top.size() >= TOP_N) break;
            System.out.printf("  %-26s %10.2f %10d %10d %10.2f%n",
                    c.symbol, c.strike, c.openInterest, c.volume, c.lastPrice);
            top.add(c);
        }
        System.out.println();

        // Phase 2: scan this OPEX's top 10 for a volume spike vs. a dynamic,
        // per-contract baseline
        scanForVolumeSpikes(provider, top, flaggedForFollowUp);
    }

    // ---------------------------------------------------------------
    // PHASE 2 — for each of the top-10 contracts, derives a per-contract
    // dynamic floor and a robust (median/MAD) z-score from trailing
    // volume history, then flags outliers that clear both.
    // No follow-up action is taken here; flagged contracts are only
    // collected for the end-of-run summary (Phase 3 will act on them).
    // ---------------------------------------------------------------
    private static void scanForVolumeSpikes(OptionsDataProvider provider, List<OptionContract> topContracts,
                                             List<OptionContract> flaggedForFollowUp) {
        System.out.println("  [Phase 2] Volume-spike scan on top " + topContracts.size() + " contracts:");
        for (OptionContract c : topContracts) {
            try {
                List<Long> history = provider.getHistoricalVolumes(c.symbol, HISTORY_LOOKBACK_DAYS);

                double floor = dynamicFloor(history, c.openInterest);
                double median = median(history);
                double mad = medianAbsoluteDeviation(history, median);
                double z = MAD_Z_CONST * (c.volume - median) / Math.max(mad, MAD_MIN);

                boolean candidate = c.volume >= floor;      // clears the per-contract liquidity floor
                boolean volumeSpike = candidate && z > Z_SCORE_THRESHOLD;

                double previousClose = volumeSpike ? provider.getPreviousClose(c.symbol) : 0.0;
                boolean priceUp = volumeSpike && c.lastPrice > previousClose;
                boolean spike = volumeSpike && priceUp;   // only flag when volume AND price both moved up


                System.out.printf("    %-26s strike=%-10.2f volume=%-8d floor=%-8.1f median=%-8.1f z=%-6.1f%s%n",
                        c.symbol, c.strike, c.volume, floor, median, z, spike ? "  <<< FLAGGED" : "");

                if (spike) {
                    flaggedForFollowUp.add(c);
                }
            } catch (Exception e) {
                System.err.println("    Volume check failed for " + c.symbol + ": " + e.getMessage());
            }
            provider.pause();
        }
        System.out.println();
    }

    // ---------------------------------------------------------------
    // Per-contract dynamic floor: the largest of three liquidity signals,
    // so the floor scales with each contract's own trading behavior
    // instead of one fixed constant across every strike/expiration.
    //   - percentileFloor: this contract's own 90th-percentile volume
    //     over its trailing history (skipped if too little history)
    //   - oiFloor: a fraction of open interest, as a fallback for
    //     new/thin contracts without enough history yet
    //   - HARD_MIN_FLOOR: a small constant purely to gate total noise
    // ---------------------------------------------------------------
    private static double dynamicFloor(List<Long> history, int openInterest) {
        double percentileFloor = history.size() >= MIN_HISTORY_DAYS
                ? percentile(history, FLOOR_PERCENTILE)
                : 0.0;
        double oiFloor = openInterest * OI_FLOOR_FRACTION;
        return Math.max(Math.max(percentileFloor, oiFloor), HARD_MIN_FLOOR);
    }

    private static double median(List<Long> values) {
        if (values.isEmpty()) return 0.0;
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int n = sorted.size();
        return (n % 2 == 1) ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
    }

    private static double medianAbsoluteDeviation(List<Long> values, double median) {
        if (values.isEmpty()) return 0.0;
        List<Double> deviations = new ArrayList<>();
        for (long v : values) deviations.add(Math.abs(v - median));
        Collections.sort(deviations);
        int n = deviations.size();
        return (n % 2 == 1) ? deviations.get(n / 2) : (deviations.get(n / 2 - 1) + deviations.get(n / 2)) / 2.0;
    }

    private static double percentile(List<Long> values, double pct) {
        if (values.isEmpty()) return 0.0;
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int idx = (int) Math.ceil(pct / 100.0 * sorted.size()) - 1;
        idx = Math.max(0, Math.min(idx, sorted.size() - 1));
        return sorted.get(idx);
    }
}
