package com.OptionsScanner.Providers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
// Polygon.io provider (fallback / paid-plan path)
// NOTE: options snapshot (OI) data requires an Options plan on
// Polygon; free-tier keys will get a clear 403 here.
// =================================================================
public class PolygonProvider implements OptionsDataProvider {
    private static final String BASE_URL = "https://api.polygon.io";
    private static final String API_KEY = System.getenv().getOrDefault("POLYGON_API_KEY", "");
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    public PolygonProvider() {
        if (API_KEY.isBlank()) {
            System.err.println("WARNING: POLYGON_API_KEY is not set. "
                    + "Only unlocked demo symbols (e.g. AAPL) will work without one.");
        }
    }

    @Override
    public String name() {
        return "Polygon.io";
    }

    @Override
    public long getRateLimitPauseMs() {
        return 13_000; // free tier: ~5 requests/minute
    }

    // GET /v2/aggs/ticker/{ticker}/prev
    @Override
    public double getLastClose(String ticker) throws Exception {
        String url = String.format("%s/v2/aggs/ticker/%s/prev?adjusted=true&apiKey=%s",
                BASE_URL, ticker, API_KEY);
        JSONObject resp = getJson(url);
        JSONArray results = resp.optJSONArray("results");
        if (results == null || results.isEmpty()) {
            throw new RuntimeException("No previous-close data returned for " + ticker + ": " + resp);
        }
        return results.getJSONObject(0).getDouble("c");
    }

    // GET /v3/snapshot/options/{underlyingAsset}
    @Override
    public List<OptionContract> getCallChain(String ticker, LocalDate expiration, double minStrike, double maxStrike) throws Exception {
        String expStr = expiration.format(DateTimeFormatter.ISO_LOCAL_DATE);
        List<OptionContract> out = new ArrayList<>();

        String url = String.format(
                "%s/v3/snapshot/options/%s?contract_type=call&expiration_date=%s&strike_price.gt=%.2f&strike_price.lt=%.2f&limit=250&apiKey=%s",
                BASE_URL, ticker, expStr, minStrike, maxStrike, API_KEY);

        while (url != null) {
            JSONObject resp = getJson(url);
            if ("ERROR".equalsIgnoreCase(resp.optString("status", ""))) {
                throw new RuntimeException("Polygon API error: " + resp.optString("error", resp.toString()));
            }

            JSONArray results = resp.optJSONArray("results");
            if (results != null) {
                for (int i = 0; i < results.length(); i++) {
                    JSONObject c = results.getJSONObject(i);
                    JSONObject details = c.optJSONObject("details");
                    JSONObject day = c.optJSONObject("day");

                    out.add(new OptionContract(
                            details != null ? details.optString("ticker", "?") : "?",
                            details != null ? details.optDouble("strike_price", 0.0) : 0.0,
                            c.optInt("open_interest", 0),
                            day != null ? day.optLong("volume", 0) : 0,
                            day != null ? day.optDouble("close", 0.0) : 0.0
                    ));
                }
            }

            String next = resp.optString("next_url", null);
            url = (next == null || next.isBlank()) ? null : next + "&apiKey=" + API_KEY;
            if (url != null) pause(); // stay within rate limit while paginating
        }
        return out;
    }

    // GET /v2/aggs/ticker/{optionsTicker}/range/1/day/{from}/{to}
    // One call returns the whole trailing window, so this is far
    // cheaper on Polygon's rate limit than the day-by-day walk used
    // for MarketData.app.
    @Override
    public List<Long> getHistoricalVolumes(String optionSymbol, int lookbackDays) throws Exception {
        LocalDate to = LocalDate.now().minusDays(1);
        LocalDate from = to.minusDays((long) lookbackDays * 2); // buffer for weekends/holidays

        String url = String.format(
                "%s/v2/aggs/ticker/%s/range/1/day/%s/%s?adjusted=true&sort=desc&limit=%d&apiKey=%s",
                BASE_URL, optionSymbol,
                from.format(DateTimeFormatter.ISO_LOCAL_DATE), to.format(DateTimeFormatter.ISO_LOCAL_DATE),
                lookbackDays, API_KEY);

        JSONObject resp = getJson(url);
        JSONArray results = resp.optJSONArray("results");
        if (results == null || results.isEmpty()) {
            return Collections.emptyList();
        }

        int n = Math.min(results.length(), lookbackDays);
        List<Long> volumes = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            volumes.add(results.getJSONObject(i).optLong("v", 0));
        }
        return volumes;
    }

    // GET /v2/aggs/ticker/{optionsTicker}/prev  (same pattern used for the underlying's last close)
    @Override
    public double getPreviousClose(String optionSymbol) throws Exception {
        String url = String.format("%s/v2/aggs/ticker/%s/prev?adjusted=true&apiKey=%s",
                BASE_URL, optionSymbol, API_KEY);
        JSONObject resp = getJson(url);
        JSONArray results = resp.optJSONArray("results");
        if (results == null || results.isEmpty()) {
            return 0.0;
        }
        return results.getJSONObject(0).optDouble("c", 0.0);
    }

    private JSONObject getJson(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());

        if (resp.statusCode() == 403) {
            throw new RuntimeException("HTTP 403 Forbidden — your Polygon plan does not include this "
                    + "endpoint (common on free-tier keys for options snapshot/OI data). URL: " + url);
        }
        if (resp.statusCode() == 429) {
            throw new RuntimeException("HTTP 429 Too Many Requests — rate limit exceeded.");
        }
        if (resp.statusCode() != 200) {
            throw new RuntimeException("HTTP " + resp.statusCode() + " from Polygon: " + resp.body());
        }
        return new JSONObject(resp.body());
    }
}