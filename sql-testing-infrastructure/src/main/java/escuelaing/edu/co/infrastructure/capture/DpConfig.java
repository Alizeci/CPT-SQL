package escuelaing.edu.co.infrastructure.capture;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configuration of the differentially-private release computed at the end of Phase 2.
 *
 * <p>Read from environment variables so that it works identically when the capture
 * runs inside a Spring context or from a plain {@code main} (e.g. the simulator):</p>
 * <pre>
 * CPT_DP_BOUNDS=price:0:600,rating:0:5,stock_quantity:0:1000   # declared clipping bounds
 * CPT_DP_EPSILON_NUMERIC=0.6
 * CPT_DP_EPSILON_CATEGORICAL=0.4
 * CPT_DP_DELTA=1e-6
 * CPT_DP_KEEP_SANITIZED_ROWS=false    # debugging only — keeps raw sanitized rows in the profile
 * </pre>
 *
 * <p>Bounds must be chosen from domain knowledge, never from the captured data:
 * observed extremes would themselves leak individual values. Numeric columns without
 * declared bounds are suppressed (not released).</p>
 */
public record DpConfig(Map<String, double[]> bounds,
                       double epsilonNumeric,
                       double epsilonCategorical,
                       double delta,
                       boolean keepSanitizedRows) {

    public static final double DEFAULT_EPSILON_NUMERIC     = 0.6;
    public static final double DEFAULT_EPSILON_CATEGORICAL = 0.4;
    public static final double DEFAULT_DELTA               = 1e-6;

    public static DpConfig defaults() {
        return new DpConfig(Collections.emptyMap(), DEFAULT_EPSILON_NUMERIC,
                DEFAULT_EPSILON_CATEGORICAL, DEFAULT_DELTA, false);
    }

    public static DpConfig fromEnv() {
        return new DpConfig(
                parseBounds(env("CPT_DP_BOUNDS", "")),
                Double.parseDouble(env("CPT_DP_EPSILON_NUMERIC", String.valueOf(DEFAULT_EPSILON_NUMERIC))),
                Double.parseDouble(env("CPT_DP_EPSILON_CATEGORICAL", String.valueOf(DEFAULT_EPSILON_CATEGORICAL))),
                Double.parseDouble(env("CPT_DP_DELTA", String.valueOf(DEFAULT_DELTA))),
                Boolean.parseBoolean(env("CPT_DP_KEEP_SANITIZED_ROWS", "false")));
    }

    /** Parses {@code "col:lo:hi,col2:lo:hi"} (column names are lower-cased). */
    public static Map<String, double[]> parseBounds(String raw) {
        Map<String, double[]> out = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) return out;
        for (String part : raw.split(",")) {
            String[] f = part.trim().split(":");
            if (f.length != 3) {
                throw new IllegalArgumentException("Invalid DP bound '" + part + "' — expected col:lower:upper");
            }
            double lo = Double.parseDouble(f[1].trim());
            double hi = Double.parseDouble(f[2].trim());
            if (!(hi > lo)) {
                throw new IllegalArgumentException("Invalid DP bound '" + part + "' — upper must be > lower");
            }
            out.put(f[0].trim().toLowerCase(), new double[]{lo, hi});
        }
        return out;
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) v = System.getProperty(key);
        return (v == null || v.isBlank()) ? def : v;
    }
}
