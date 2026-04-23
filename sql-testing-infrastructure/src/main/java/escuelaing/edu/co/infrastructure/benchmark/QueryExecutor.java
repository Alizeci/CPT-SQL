package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.TestProfile;
import escuelaing.edu.co.domain.model.TransactionRecord;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
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

    private final Random rng = new Random();

    /** String parameter pool populated from the load profile before the benchmark runs. */
    private List<String> stringPool = Collections.emptyList();

    /** Sets the string parameter pool built from production sanitized data. */
    public void setStringPool(List<String> pool) {
        this.stringPool = pool != null ? pool : Collections.emptyList();
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

        String plan   = captureExplainPlan(conn, resolvedSql);
        Instant start = Instant.now();
        long startNs  = System.nanoTime();

        try (PreparedStatement ps = conn.prepareStatement(resolvedSql)) {
            bindSyntheticParameters(ps, accessDist, zipfAlpha);
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
     * <p>String parameters receive a random sample value. Integer parameters use
     * UNIFORM or ZIPF distribution: with ZIPF, lower IDs are selected more
     * frequently — P(k) ∝ k^{-α}.</p>
     */
    private void bindSyntheticParameters(PreparedStatement ps,
                                          TestProfile.AccessDistribution dist,
                                          double zipfAlpha) throws SQLException {
        try {
            java.sql.ParameterMetaData meta = ps.getParameterMetaData();
            for (int i = 1; i <= meta.getParameterCount(); i++) {
                int sqlType;
                try {
                    sqlType = meta.getParameterType(i);
                } catch (SQLException ex) {
                    sqlType = java.sql.Types.INTEGER;
                }
                switch (sqlType) {
                    case java.sql.Types.VARCHAR,
                         java.sql.Types.CHAR,
                         java.sql.Types.LONGVARCHAR,
                         java.sql.Types.NVARCHAR ->
                            ps.setString(i, randomString());
                    default -> {
                        int value = (dist == TestProfile.AccessDistribution.ZIPF)
                                ? zipfInt(500, zipfAlpha)
                                : syntheticInt(1, 500);
                        ps.setInt(i, value);
                    }
                }
            }
        } catch (SQLException ignored) {
            // Some JDBC drivers throw on getParameterMetaData — safe to skip binding
        }
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

    private String captureExplainPlan(Connection conn, String sql) {
        try (PreparedStatement ps = conn.prepareStatement("EXPLAIN ANALYZE " + sql)) {
            StringBuilder sb = new StringBuilder();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) sb.append(rs.getString(1)).append('\n');
            }
            return sb.toString();
        } catch (SQLException e) {
            LOG.fine("[QueryExecutor] EXPLAIN ANALYZE unavailable: " + e.getMessage());
            return null;
        }
    }

    /**
     * Parses the estimated total cost from an EXPLAIN ANALYZE output.
     * PostgreSQL reports {@code cost=X..Y} on the root node; this method returns Y.
     *
     * @return estimated total cost, or 0 if the output cannot be parsed
     */
    public static double extractPlanCost(String explainOutput) {
        if (explainOutput == null || explainOutput.isBlank()) return 0.0;
        int idx = explainOutput.indexOf("cost=");
        if (idx < 0) return 0.0;
        try {
            String sub = explainOutput.substring(idx + 5);
            int dotDot = sub.indexOf("..");
            int space  = sub.indexOf(' ');
            if (dotDot < 0 || space < 0) return 0.0;
            return Double.parseDouble(sub.substring(dotDot + 2, space));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}
