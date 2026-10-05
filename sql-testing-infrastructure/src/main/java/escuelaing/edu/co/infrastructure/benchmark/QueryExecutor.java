package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.TestProfile;
import escuelaing.edu.co.domain.model.TransactionRecord;
import escuelaing.edu.co.infrastructure.dialect.DatabaseDialect;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Executes a single SQL query against the mirror database and returns a
 * {@link TransactionRecord} with the measured latency and execution plan cost.
 *
 * <p>Responsibilities: resolve the SQL to run (captured SQL from Phase 2 takes
 * precedence), capture the planner cost via {@code EXPLAIN ANALYZE}, bind
 * synthetic parameters according to the access distribution, execute the
 * statement, and record the elapsed time.</p>
 *
 * <p>If no SQL is available for a query (neither captured nor provided), the
 * execution is skipped with a warning rather than falling back to a generic
 * statement unrelated to the user's schema.</p>
 */
@Component
public class QueryExecutor {

    private static final Logger LOG = Logger.getLogger(QueryExecutor.class.getName());

    private static final String ALPHANUM =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    private final DatabaseDialect adapter;

    /**
     * Seeded generator for synthetic parameters. Reseeded at the start of each run
     * ({@link #reseed}) so that the parameter sequence is reproducible across runs.
     */
    private Random rng = new Random(42);

    public QueryExecutor(DatabaseDialect adapter) {
        this.adapter = adapter;
    }

    /** String parameter pool populated from the load profile before the benchmark runs. */
    private List<String> stringPool = Collections.emptyList();

    /** Sets the string parameter pool built from production sanitized data. */
    public void setStringPool(List<String> pool) {
        this.stringPool = pool != null ? pool : Collections.emptyList();
    }

    /** Numeric column model (col → [mean, std, lower, upper]) from the DP release. */
    private Map<String, double[]> columnStats = Collections.emptyMap();

    /** Categorical value pools per column from the DP release. */
    private Map<String, List<String>> columnPools = Collections.emptyMap();

    /**
     * Sets the per-column parameter model derived from the DP release, so that
     * parameters bound to a known column (e.g. {@code price BETWEEN ? AND ?},
     * {@code category = ?}) follow the released distribution of that column instead
     * of a generic uniform range. Columns without a model keep the previous behaviour.
     */
    public void setParameterModel(Map<String, double[]> stats, Map<String, List<String>> pools) {
        this.columnStats = stats != null ? stats : Collections.emptyMap();
        this.columnPools = pools != null ? pools : Collections.emptyMap();
    }

    /** Resets the parameter generator so that runs with the same seed bind the same values. */
    public void reseed(long seed) {
        this.rng = new Random(seed);
    }

    /**
     * Binds synthetic parameters to {@code ps} (uniform access distribution) and returns them.
     * Used by fidelity validation to execute captured write statements.
     */
    public List<Object> bindParameters(PreparedStatement ps, String sql) {
        return collectAndBind(ps, sql, TestProfile.AccessDistribution.UNIFORM, 1.0);
    }

    // Public API

    public TransactionRecord execute(Connection conn,
                                     String queryId,
                                     LoadProfile.QueryStats stats,
                                     String sql) {
        return execute(conn, queryId, stats, sql,
                TestProfile.AccessDistribution.UNIFORM, 1.0);
    }

    /**
     * Executes the query identified by {@code queryId} against the mirror database.
     *
     * <p>With {@link TestProfile.AccessDistribution#ZIPF}, integer parameters are
     * skewed toward lower IDs so hot records receive more traffic than cold ones.</p>
     *
     * @param sql explicit SQL to run; if null, falls back to {@code stats.getCapturedSql()}
     * @return record with measured latency and captured execution plan
     */
    public TransactionRecord execute(Connection conn,
                                     String queryId,
                                     LoadProfile.QueryStats stats,
                                     String sql,
                                     TestProfile.AccessDistribution accessDist,
                                     double zipfAlpha) {
        String resolvedSql = resolveSql(sql, stats);
        if (resolvedSql == null) {
            LOG.warning("[QueryExecutor] No SQL available for '" + queryId + "' — skipping.");
            return TransactionRecord.builder()
                    .queryId(queryId).sql("").latencyMs(0).timestamp(Instant.now()).build();
        }

        String  plan    = null;
        Instant start   = Instant.now();
        long    startNs = System.nanoTime();

        try (PreparedStatement ps = conn.prepareStatement(resolvedSql)) {
            List<Object> params = collectAndBind(ps, resolvedSql, accessDist, zipfAlpha);
            plan    = captureExplainPlan(conn, resolvedSql, params);
            start   = Instant.now();
            startNs = System.nanoTime();

            String verb = resolvedSql.strip().toUpperCase();
            boolean isDml = verb.startsWith("UPDATE") || verb.startsWith("DELETE")
                    || (verb.startsWith("INSERT") && !verb.contains("RETURNING"));
            if (isDml) {
                ps.executeUpdate();
            } else {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {}
                }
            }
        } catch (SQLException e) {
            LOG.warning("[QueryExecutor] Error executing '" + queryId + "': " + e.getMessage());
        }

        long latencyMs = (System.nanoTime() - startNs) / 1_000_000;

        return TransactionRecord.builder()
                .queryId(queryId)
                .sql(resolvedSql)
                .latencyMs(latencyMs)
                .timestamp(start)
                .executionPlan(plan)
                .build();
    }

    // SQL resolution

    private String resolveSql(String sql, LoadProfile.QueryStats stats) {
        if (sql != null && !sql.isBlank()) return sql;
        if (stats != null && stats.getCapturedSql() != null && !stats.getCapturedSql().isBlank()) {
            return stats.getCapturedSql();
        }
        return null;
    }

    // Parameter binding

    /**
     * Binds synthetic parameters to the prepared statement.
     *
     * <p>Each placeholder is first mapped to the column it is compared with
     * ({@link #mapParametersToColumns}). When the DP release provides a model for that
     * column, the value is drawn from it: numeric columns from N(mean, std) clamped to the
     * declared bounds (for {@code BETWEEN ? AND ?} two values are drawn and ordered), string
     * columns from the released histogram. Identifier columns and columns without a model
     * fall back to the original behaviour: integers in [1, 500] (UNIFORM or ZIPF) and strings
     * from the shared pool.</p>
     */
    private List<Object> collectAndBind(PreparedStatement ps,
                                        String sql,
                                        TestProfile.AccessDistribution dist,
                                        double zipfAlpha) {
        List<Object> params = new ArrayList<>();
        try {
            java.sql.ParameterMetaData meta = ps.getParameterMetaData();
            int count = meta.getParameterCount();
            List<ParamSlot> slots = mapParametersToColumns(sql, count);
            Object pendingHigh = null;
            for (int i = 1; i <= count; i++) {
                int sqlType;
                try {
                    sqlType = meta.getParameterType(i);
                } catch (SQLException ex) {
                    sqlType = java.sql.Types.INTEGER;
                }
                ParamSlot slot = slots.get(i - 1);
                Object value;
                if (slot.role == Role.BETWEEN_HIGH && pendingHigh != null) {
                    value = pendingHigh;
                    pendingHigh = null;
                } else if (slot.column != null && columnStats.containsKey(slot.column)
                        && !slot.column.endsWith("id")) {
                    double[] m = columnStats.get(slot.column);
                    if (slot.role == Role.BETWEEN_LOW) {
                        double a = sampleColumn(m), b = sampleColumn(m);
                        value       = typed(Math.min(a, b), sqlType);
                        pendingHigh = typed(Math.max(a, b), sqlType);
                    } else if (slot.role == Role.INCREMENT) {
                        value = typed(1 + rng.nextInt(5), sqlType);
                    } else {
                        value = typed(sampleColumn(m), sqlType);
                    }
                } else if (slot.column != null && columnPools.containsKey(slot.column)
                        && !columnPools.get(slot.column).isEmpty()) {
                    List<String> pool = columnPools.get(slot.column);
                    value = pool.get(rng.nextInt(pool.size()));
                } else {
                    switch (sqlType) {
                        case java.sql.Types.VARCHAR,
                             java.sql.Types.CHAR,
                             java.sql.Types.LONGVARCHAR,
                             java.sql.Types.NVARCHAR -> value = randomString();
                        default -> value = (dist == TestProfile.AccessDistribution.ZIPF)
                                ? zipfInt(500, zipfAlpha)
                                : syntheticInt(1, 500);
                    }
                }
                ps.setObject(i, value);
                params.add(value);
            }
        } catch (SQLException ignored) {
            // Some JDBC drivers throw on getParameterMetaData — safe to skip binding
        }
        return params;
    }

    private double sampleColumn(double[] model) {
        double v = model[0] + rng.nextGaussian() * model[1];
        return Math.max(model[2], Math.min(model[3], v));
    }

    private static Object typed(double v, int sqlType) {
        return switch (sqlType) {
            case java.sql.Types.INTEGER, java.sql.Types.SMALLINT, java.sql.Types.TINYINT -> (int) Math.round(v);
            case java.sql.Types.BIGINT -> Math.round(v);
            case java.sql.Types.NUMERIC, java.sql.Types.DECIMAL ->
                    java.math.BigDecimal.valueOf(Math.round(v * 100) / 100.0);
            default -> v;
        };
    }

    // Placeholder → column mapping

    enum Role { COMPARE, BETWEEN_LOW, BETWEEN_HIGH, INCREMENT, INSERT_VALUE, UNKNOWN }

    record ParamSlot(String column, Role role) {}

    private static final java.util.regex.Pattern BETWEEN_LOW_P = java.util.regex.Pattern.compile(
            "(?i)(?:\\w+\\.)?(\\w+)\\s+BETWEEN\\s*$");
    private static final java.util.regex.Pattern BETWEEN_HIGH_P = java.util.regex.Pattern.compile(
            "(?i)(?:\\w+\\.)?(\\w+)\\s+BETWEEN\\s*\\?\\s+AND\\s*$");
    private static final java.util.regex.Pattern INCREMENT_P = java.util.regex.Pattern.compile(
            "(?i)(?:\\w+\\.)?(\\w+)\\s*[+-]\\s*$");
    private static final java.util.regex.Pattern COMPARE_P = java.util.regex.Pattern.compile(
            "(?i)(?:\\w+\\.)?(\\w+)\\s*(?:=|<>|!=|<=|>=|<|>|\\bLIKE\\b)\\s*$");
    private static final java.util.regex.Pattern INSERT_P = java.util.regex.Pattern.compile(
            "(?is)INSERT\\s+INTO\\s+\\S+\\s*\\(([^)]*)\\)\\s*VALUES\\s*\\(([^)]*)\\)");

    /**
     * Maps each {@code ?} placeholder of {@code sql} (in order) to the column it refers to.
     * Recognises {@code col = ?} (and other comparisons), {@code col BETWEEN ? AND ?},
     * {@code col = col + ?} and positional {@code INSERT INTO t (cols) VALUES (...)}.
     * Unrecognised placeholders get {@link Role#UNKNOWN}.
     */
    static List<ParamSlot> mapParametersToColumns(String sql, int expected) {
        List<ParamSlot> slots = new ArrayList<>();
        if (sql != null) {
            java.util.regex.Matcher ins = INSERT_P.matcher(sql);
            List<String> insertCols = new ArrayList<>();
            if (ins.find()) {
                String[] cols = ins.group(1).split(",");
                String[] vals = ins.group(2).split(",");
                for (int k = 0; k < vals.length && k < cols.length; k++) {
                    if (vals[k].trim().equals("?")) insertCols.add(cols[k].trim().toLowerCase());
                }
            }
            int insertIdx = 0;
            for (int pos = sql.indexOf('?'); pos >= 0; pos = sql.indexOf('?', pos + 1)) {
                String before = sql.substring(Math.max(0, pos - 120), pos);
                java.util.regex.Matcher m;
                if ((m = BETWEEN_HIGH_P.matcher(before)).find()) {
                    slots.add(new ParamSlot(m.group(1).toLowerCase(), Role.BETWEEN_HIGH));
                } else if ((m = BETWEEN_LOW_P.matcher(before)).find()) {
                    slots.add(new ParamSlot(m.group(1).toLowerCase(), Role.BETWEEN_LOW));
                } else if ((m = INCREMENT_P.matcher(before)).find()) {
                    slots.add(new ParamSlot(m.group(1).toLowerCase(), Role.INCREMENT));
                } else if ((m = COMPARE_P.matcher(before)).find()) {
                    slots.add(new ParamSlot(m.group(1).toLowerCase(), Role.COMPARE));
                } else if (insertIdx < insertCols.size()) {
                    slots.add(new ParamSlot(insertCols.get(insertIdx++), Role.INSERT_VALUE));
                } else {
                    slots.add(new ParamSlot(null, Role.UNKNOWN));
                }
            }
        }
        while (slots.size() < expected) slots.add(new ParamSlot(null, Role.UNKNOWN));
        return slots;
    }

    /**
     * Returns a string value from the pool if available, or a random alphanumeric
     * string of length 6–12 as fallback.
     */
    private String randomString() {
        if (!stringPool.isEmpty()) return stringPool.get(rng.nextInt(stringPool.size()));
        int len = 6 + rng.nextInt(7);
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) sb.append(ALPHANUM.charAt(rng.nextInt(ALPHANUM.length())));
        return sb.toString();
    }

    /**
     * Returns a Zipf-distributed integer in [1, n].
     *
     * <p>P(k) ∝ k^{-α}: lower values are selected more often, simulating hot-record access.
     * Uses inverse CDF sampling — O(n) per call, acceptable for n≤1000.</p>
     */
    private int zipfInt(int n, double alpha) {
        double sum = 0;
        for (int i = 1; i <= n; i++) sum += 1.0 / Math.pow(i, alpha);
        double target = rng.nextDouble() * sum;
        double cumulative = 0;
        for (int i = 1; i <= n; i++) {
            cumulative += 1.0 / Math.pow(i, alpha);
            if (cumulative >= target) return i;
        }
        return n;
    }

    private int syntheticInt(int min, int max) {
        return min + rng.nextInt(max - min + 1);
    }

    // EXPLAIN ANALYZE

    private String captureExplainPlan(Connection conn, String sql, List<Object> params) {
        try (PreparedStatement ps = conn.prepareStatement(adapter.buildExplainAnalyze(sql))) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            StringBuilder sb = new StringBuilder();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) sb.append(rs.getString(1)).append('\n');
            }
            return sb.toString();
        } catch (SQLException e) {
            LOG.warning("[QueryExecutor] EXPLAIN ANALYZE failed for query: " + e.getMessage());
            return null;
        }
    }

    /** Delegates to {@link DatabaseDialect#parsePlanCost}. */
    public double extractPlanCost(String explainOutput) {
        return adapter.parsePlanCost(explainOutput);
    }
}
