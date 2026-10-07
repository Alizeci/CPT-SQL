package escuelaing.edu.co.infrastructure.capture;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Privacy budgets and declarations for the differentially private release computed at the
 * end of a capture.
 *
 * <p>{@link #fromEnv()} reads environment variables (falling back to JVM system properties
 * of the same name), so the configuration is the same inside Spring or a plain
 * {@code main}. Values shown are the defaults, except the examples for lists:</p>
 * <pre>
 * CPT_DP_BOUNDS=price:0:600,rating:0:5        # column:lower:upper clipping bounds (none by default)
 * CPT_DP_EPSILON_NUMERIC=0.6
 * CPT_DP_EPSILON_CATEGORICAL=0.4
 * CPT_DP_EPSILON_FK=0.4                       # foreign-key concentration curves; 0 disables them
 * CPT_DP_DELTA=1e-6
 * CPT_DP_CATEGORICAL=products.category        # table.column list for the table-level release
 * CPT_DP_KEEP_SANITIZED_ROWS=false            # debugging only; raw sanitized rows, no DP guarantee
 * </pre>
 *
 * <p>Bounds are keyed by column name alone and apply to that column in every table. They
 * must come from domain knowledge, never from the captured data: observed extremes would
 * themselves leak individual values. Numeric columns without declared bounds are suppressed
 * (not released).</p>
 *
 * <p>By sequential composition the overall guarantee is
 * {@code (ε_numeric + ε_categorical + ε_fk, 2·δ)}: numeric and categorical releases share
 * one {@code δ}, and the foreign-key release spends its own.</p>
 */
public record DpConfig(Map<String, double[]> bounds,
                       double epsilonNumeric,
                       double epsilonCategorical,
                       double delta,
                       boolean keepSanitizedRows,
                       double epsilonForeignKey,
                       java.util.List<String> categoricalColumns) {

    public static final double DEFAULT_EPSILON_NUMERIC     = 0.6;
    public static final double DEFAULT_EPSILON_CATEGORICAL = 0.4;
    public static final double DEFAULT_DELTA               = 1e-6;
    public static final double DEFAULT_EPSILON_FK          = 0.4;

    /** Uses the default foreign-key budget and no declared categorical columns. */
    public DpConfig(Map<String, double[]> bounds, double epsilonNumeric, double epsilonCategorical,
                    double delta, boolean keepSanitizedRows) {
        this(bounds, epsilonNumeric, epsilonCategorical, delta, keepSanitizedRows, DEFAULT_EPSILON_FK,
                java.util.List.of());
    }

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
                Boolean.parseBoolean(env("CPT_DP_KEEP_SANITIZED_ROWS", "false")),
                Double.parseDouble(env("CPT_DP_EPSILON_FK", String.valueOf(DEFAULT_EPSILON_FK))),
                parseColumns(env("CPT_DP_CATEGORICAL", "")));
    }

    /**
     * Parses {@code "table.col,table2.col2"} (lower-cased, blanks ignored).
     *
     * @throws IllegalArgumentException if an entry is not a plain {@code table.column}
     */
    public static java.util.List<String> parseColumns(String raw) {
        java.util.List<String> out = new java.util.ArrayList<>();
        if (raw == null || raw.isBlank()) return out;
        for (String part : raw.split(",")) {
            String p = part.trim().toLowerCase();
            if (p.isEmpty()) continue;
            if (!p.matches("[a-z_][a-z0-9_]*\\.[a-z_][a-z0-9_]*")) {
                throw new IllegalArgumentException("Invalid categorical column '" + part + "' — expected table.column");
            }
            out.add(p);
        }
        return java.util.Collections.unmodifiableList(out);
    }

    /**
     * Parses {@code "col:lo:hi,col2:lo:hi"} (column names are lower-cased).
     *
     * @throws IllegalArgumentException if an entry is malformed or {@code hi <= lo}
     */
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
