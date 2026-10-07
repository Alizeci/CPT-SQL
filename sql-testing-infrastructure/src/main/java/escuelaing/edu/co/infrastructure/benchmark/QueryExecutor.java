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
 * <p>Responsibilities: resolve the SQL to run (explicit SQL, else the captured SQL from
 * the load profile), bind synthetic parameters according to the access distribution,
 * capture the plan via {@code EXPLAIN ANALYZE} (not included in the measured latency),
 * execute the statement, and record the elapsed time.</p>
 *
 * <p>If no SQL is available for a query, the execution is skipped with a warning and a
 * record with empty SQL and 0 ms latency is returned, rather than running a generic
 * statement unrelated to the user's schema.</p>
 *
 * <p>Not thread-safe: the parameter generator and counters are shared mutable state.</p>
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

    /**
     * Per-statement timeout on the mirror database in ms (default 60 000; rounded up to
     * whole seconds, minimum 1 s). Also applies to the {@code EXPLAIN ANALYZE} call.
     *
     * <p>A severely degraded query must produce a measurable result, not hang the job until
     * the CI timeout kills it and every measurement is lost. When the limit is reached the
     * statement is cancelled and recorded with a latency of at least the limit and a
     * "severe degradation" warning, so it counts against p95 and SLA compliance.</p>
     */
    @org.springframework.beans.factory.annotation.Value("${loadtest.benchmark.statementTimeoutMs:60000}")
    private long statementTimeoutMs = 60_000;

    /** Number of statements cancelled by the timeout since this bean was created. */
    private long timedOutStatements = 0;

    public long getTimedOutStatements() { return timedOutStatements; }

    private int timeoutSeconds() {
        return (int) Math.max(1, Math.ceil(statementTimeoutMs / 1000.0));
    }

    private static boolean isTimeout(SQLException e) {
        return e instanceof java.sql.SQLTimeoutException || "57014".equals(e.getSQLState());
    }

    /** String parameter pool populated from the load profile before the benchmark runs. */
    private List<String> stringPool = Collections.emptyList();

    /**
     * Sets the fallback string pool (from the differentially private release, or from
     * sanitized rows for legacy profiles); {@code null} clears it.
     */
    public void setStringPool(List<String> pool) {
        this.stringPool = pool != null ? pool : Collections.emptyList();
    }

    /** Numeric column model (col → [mean, std, lower, upper]) used to generate the mirror. */
    private Map<String, double[]> columnStats = Collections.emptyMap();

    /** Categorical value pools per column used to generate the mirror. */
    private Map<String, List<String>> columnPools = Collections.emptyMap();

    /**
     * Sets the per-column parameter model used to generate the mirror database, so that
     * parameters bound to a known column (e.g. {@code price BETWEEN ? AND ?},
     * {@code category = ?}) follow that column's distribution instead of a generic
     * range. Columns without a model get generic integers or strings from the pool.
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

    /** Same as the full overload with UNIFORM access distribution. */
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
     * <p>SQL errors other than a timeout are logged and the record keeps the elapsed
     * time; no exception is thrown.</p>
     *
     * @param sql explicit SQL to run; if null or blank, falls back to {@code stats.getCapturedSql()}
     * @return record with measured latency (ms) and captured execution plan ({@code null}
     *         if EXPLAIN failed)
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

        boolean timedOut = false;
        try (PreparedStatement ps = conn.prepareStatement(resolvedSql)) {
            ps.setQueryTimeout(timeoutSeconds());
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
            if (isTimeout(e)) {
                timedOut = true;
                timedOutStatements++;
            } else {
                LOG.warning("[QueryExecutor] Error executing '" + queryId + "': " + e.getMessage());
            }
        }

        long latencyMs = (System.nanoTime() - startNs) / 1_000_000;
        if (timedOut) {
            // Record at least the limit so the degradation is visible in p95 / SLA compliance.
            latencyMs = Math.max(latencyMs, statementTimeoutMs);
            LOG.warning("[QueryExecutor] '" + queryId + "' cancelled after " + statementTimeoutMs
                    + " ms (statement timeout) — recorded as " + latencyMs + " ms (severe degradation).");
        }

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
     * ({@link #mapParametersToColumns}). When a model exists for that column, the value is
     * drawn from it: numeric columns from N(mean, std) clamped to the declared bounds (for
     * {@code BETWEEN ? AND ?} two values are drawn and ordered; for {@code col = col + ?}
     * an increment in [1, 5]), string columns from the column's value pool. Columns whose
     * name ends in {@code id} skip the numeric model. Unmodelled parameters get integers in
     * [1, 500] (UNIFORM or ZIPF) or strings from the shared pool.</p>
     *
     * <p>If the driver cannot report parameter metadata, nothing is bound.</p>
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
     * Uses inverse CDF sampling — O(n) per call, acceptable for n ≤ 1000.</p>
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

    /** Runs EXPLAIN ANALYZE with the same parameters; returns the plan text or {@code null}. */
    private String captureExplainPlan(Connection conn, String sql, List<Object> params) {
        try (PreparedStatement ps = conn.prepareStatement(adapter.buildExplainAnalyze(sql))) {
            ps.setQueryTimeout(timeoutSeconds());
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
