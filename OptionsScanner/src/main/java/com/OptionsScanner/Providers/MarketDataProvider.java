package com.OptionsScanner.Providers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import com.OptionsScanner.Data.*;
import com.OptionsScanner.Providers.Base.OptionsDataProvider;

// =================================================================
// MarketData.app provider (DEFAULT)
// Docs: https://www.marketdata.app/docs/api/options/chain
// Free Forever plan: 100 requests/day, delayed data, no credit card.
// =================================================================
public class MarketDataProvider implements OptionsDataProvider {
    private static final String BASE_URL = "https://api.marketdata.app";
    private static final String API_KEY = System.getenv().getOrDefault("MARKETDATA_API_KEY", "dUl5Vlc2TmYxNHYwdEFqdjQwMWREWjgxbmJjRVVWaHRwVmo2eGdaRGQwRT0");
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final long PAUSE_MS = 10;

    public MarketDataProvider() {
        if (API_KEY.isBlank()) {
            System.err.println("WARNING: MARKETDATA_API_KEY is not set. "
                    + "Only unlocked demo symbols (e.g. AAPL) will work without one.");
        }
    }

    @Override
    public String name() {
        return "MarketData.app (Free Forever plan)";
    }

    @Override
    public long getRateLimitPauseMs() {
        return PAUSE_MS;
         // generous plan (100/day daily window, not per-minute) — light pause is enough
    }

    // GET /v1/stocks/quotes/{symbol}/  -> columnar arrays, use prevClose
    @Override
    public double getLastClose(String ticker) throws Exception {
        String url = String.format("%s/v1/stocks/quotes/%s/", BASE_URL, ticker);
        JSONObject resp = getJson(url);
        requireOk(resp, url);

        JSONArray prevClose = resp.optJSONArray("prevClose");
        if (prevClose == null || prevClose.isEmpty()) {
            // Fallback to 'last' if prevClose isn't populated (e.g. pre-market)
            JSONArray last = resp.optJSONArray("last");
            if (last == null || last.isEmpty()) {
                throw new RuntimeException("No quote data returned for " + ticker + ": " + resp);
            }
            return last.getDouble(0);
        }
        return prevClose.getDouble(0);
    }

    // GET /v1/options/chain/{symbol}/?expiration=YYYY-MM-DD&side=call&strike=min-max
    // The API supports a closed strike interval filter, so we pass the
    // range through server-side to cut down on data transferred, then
    // still enforce the exact (exclusive) bounds client-side below.
    @Override
    public List<OptionContract> getCallChain(String ticker, LocalDate expiration, double minStrike, double maxStrike) throws Exception {

        // long sla = System.currentTimeMillis();
        String expStr = expiration.format(DateTimeFormatter.ISO_LOCAL_DATE);
        String url = String.format("%s/v1/options/chain/%s/?expiration=%s&side=call&strike=%.2f-%.2f",
                BASE_URL, ticker, expStr, minStrike, maxStrike);

        JSONObject resp = getJson(url);

        // MarketData.app returns {"s":"no_data"} (not an error) when a chain is empty
        String status = resp.optString("s", "ok");
        if ("no_data".equalsIgnoreCase(status)) {
            return Collections.emptyList();
        }
        requireOk(resp, url);

        JSONArray symbols = resp.getJSONArray("optionSymbol");
        JSONArray strikes = resp.getJSONArray("strike");
        JSONArray oi = resp.getJSONArray("openInterest");
        JSONArray volume = resp.optJSONArray("volume");
        JSONArray last = resp.optJSONArray("last");

        List<OptionContract> out = new ArrayList<>();
        for (int i = 0; i < symbols.length(); i++) {
            double strike = strikes.getDouble(i);
            if (strike <= minStrike || strike >= maxStrike) continue; // enforce: lastClose < strike < 1.5x lastClose

            out.add(new OptionContract(
                    symbols.getString(i),
                    strike,
                    oi.optInt(i, 0),
                    volume != null ? volume.optLong(i, 0) : 0,
                    last != null ? last.optDouble(i, 0.0) : 0.0
            ));
        }
        // System.out.print("getCallChain:"+(System.currentTimeMillis()-sla) + "\n");

        return out;
    }

    private void requireOk(JSONObject resp, String url) {
        String status = resp.optString("s", "ok");
        if ("error".equalsIgnoreCase(status)) {
            throw new RuntimeException("MarketData.app error for " + url + ": "
                    + resp.optString("errmsg", resp.toString()));
        }
    }

    // GET /v1/options/quotes/{optionSymbol}/?date=YYYY-MM-DD  (historical EOD quote, incl. volume)
    // NOTE: MarketData.app's documented options endpoints expose historical
    // EOD data per single date rather than a date-range candles endpoint
    // (unlike stocks/candles), so this walks back day-by-day. Weekends are
    // skipped; the walk stops once lookbackDays trading days are collected
    // or a 30-calendar-day safety bound is hit. Each day consumes one
    // request against your MarketData.app daily quota — keep lookbackDays
    // modest on the Free Forever plan (100 requests/day).
    @Override
    public List<Long> getHistoricalVolumes(String optionSymbol, int lookbackDays) throws Exception {
        List<Long> volumes = new ArrayList<>();
        LocalDate day = LocalDate.now().minusDays(1);
        LocalDate cutoff = LocalDate.now().minusDays(Math.max(30, lookbackDays * 2L));

        while (volumes.size() < lookbackDays && day.isAfter(cutoff)) {
            // long sla = System.currentTimeMillis();
            if (day.getDayOfWeek() != DayOfWeek.SATURDAY && day.getDayOfWeek() != DayOfWeek.SUNDAY) {
                String url = String.format("%s/v1/options/quotes/%s/?date=%s",
                        BASE_URL, optionSymbol, day.format(DateTimeFormatter.ISO_LOCAL_DATE));
                try {
                    JSONObject resp = getJson(url);
                    if ("ok".equalsIgnoreCase(resp.optString("s", "ok"))) {
                        JSONArray vol = resp.optJSONArray("volume");
                        if (vol != null && !vol.isEmpty()) {
                            volumes.add(vol.optLong(0, 0));
                        }
                    }
                } catch (Exception e) {
                    // Missing/invalid historical day for this contract — skip it, don't fail the scan
                }
                pause();
            }
            day = day.minusDays(1);
            // System.out.print("getHistoricalVolumes:"+(System.currentTimeMillis()-sla) + "\n");
        }
        return volumes;
    }

    // GET /v1/options/quotes/{optionSymbol}/?date=YYYY-MM-DD  (prior trading day's EOD close)
    @Override
    public double getPreviousClose(String optionSymbol) throws Exception {
        // long sla = System.currentTimeMillis();

        LocalDate day = LocalDate.now().minusDays(1);
        while (day.getDayOfWeek() == DayOfWeek.SATURDAY || day.getDayOfWeek() == DayOfWeek.SUNDAY) {
            day = day.minusDays(1);
        }
        String url = String.format("%s/v1/options/quotes/%s/?date=%s",
                BASE_URL, optionSymbol, day.format(DateTimeFormatter.ISO_LOCAL_DATE));
        JSONObject resp = getJson(url);
        if (!"ok".equalsIgnoreCase(resp.optString("s", "ok"))) {
            return 0.0;
        }
        JSONArray last = resp.optJSONArray("last");
        // System.out.print("getPreviousClose:"+(System.currentTimeMillis()-sla) + "\n");
        return (last != null && !last.isEmpty()) ? last.optDouble(0, 0.0) : 0.0;
    }

    private JSONObject getJson(String url) throws Exception {
        // long sla = System.currentTimeMillis();
        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(url)).GET();
        if (!API_KEY.isBlank()) {
            builder.header("Authorization", "Bearer " + API_KEY);
        }
        HttpResponse<String> resp = HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        // System.out.print("getJson:"+(System.currentTimeMillis()-sla) + "\n");

        if (resp.statusCode() == 401 || resp.statusCode() == 403) {
            throw new RuntimeException("HTTP " + resp.statusCode()
                    + " — check MARKETDATA_API_KEY. URL: " + url);
        }
        if (resp.statusCode() == 429) {
            throw new RuntimeException("HTTP 429 — daily request limit (100/day on Free Forever) reached.");
        }
        if (resp.statusCode() != 200 && resp.statusCode() != 203) {
            throw new RuntimeException("HTTP " + resp.statusCode() + " from MarketData.app: " + resp.body() + ". URL: " + url);
        }
        return new JSONObject(resp.body());
    }
}