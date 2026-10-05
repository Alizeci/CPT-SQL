package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.DpRelease;

import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * Differentially-private release of the <em>shape</em> of each foreign-key relation of the
 * capture database: how strongly child rows concentrate on a few parent rows
 * (e.g. best-selling products owning most order items).
 *
 * <p>Per-column statistics cannot express this, and without it a synthetic mirror spreads
 * child rows uniformly across parents. Queries whose cost depends on that concentration
 * (several joins over the same parent, per-parent aggregates) then look cheap in the mirror
 * while they are expensive in production.</p>
 *
 * <h2>Mechanism</h2>
 * <ul>
 *   <li>For each relation, the degree of every parent (its number of child rows) is
 *       sorted in descending order. Only the cumulative sums at a fixed, data-independent
 *       grid of rank fractions {@link #RANK_FRACTIONS} are released.</li>
 *   <li>Neighbouring datasets differ in one child row. That changes one entry of the sorted
 *       degree sequence by one (Hay, Li, Miklau &amp; Jensen, ICDM 2009), hence every
 *       cumulative sum by at most one: L2 sensitivity of the vector is {@code √m}.</li>
 *   <li>Gaussian noise {@code σ = √m·√(2 ln(1.25/δ))/ε} per relation, with
 *       {@code (ε, δ)} split evenly across relations. The parent count is treated as public
 *       (it does not change when a child row is added or removed).</li>
 *   <li>Post-processing (no extra budget): per-segment average degrees are clamped at zero
 *       and made non-increasing (pool-adjacent-violators), then normalised to shares.</li>
 * </ul>
 *
 * <p>Parent identifiers and individual degrees are never released. The generator assumes
 * that a parent's popularity is shared across its child tables (the same products are hot
 * in {@code order_items} and {@code inventory_log}); that assumption is not learnt from
 * data and is documented as such.</p>
 */
public final class FkDegreeProfiler {

    private static final Logger LOG = Logger.getLogger(FkDegreeProfiler.class.getName());

    /** Public grid of parent-rank fractions (top 0.001 %, 0.01 %, … , 100 %). */
    public static final double[] RANK_FRACTIONS = {1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1.0};

    /** Mechanism description appended to the release for auditability. */
    public static final String MECHANISM =
            "foreign keys: Gaussian mechanism on cumulative sorted-degree sums at fixed rank fractions "
          + "(L2 sensitivity sqrt(m)), post-processed to a non-increasing concentration curve";

    private FkDegreeProfiler() {}

    /** Profiles every single-column foreign key of the {@code public} schema. */
    public static Map<String, DpRelease.ForeignKeyDegree> profile(Connection conn, DpConfig config)
            throws SQLException {
        return profile(conn, config, new SecureRandom());
    }

    static Map<String, DpRelease.ForeignKeyDegree> profile(Connection conn, DpConfig config, Random noise)
            throws SQLException {
        Map<String, DpRelease.ForeignKeyDegree> out = new TreeMap<>();
        if (config.epsilonForeignKey() <= 0) return out;

        List<String[]> relations = discoverForeignKeys(conn);
        if (relations.isEmpty()) return out;
        double eps = config.epsilonForeignKey() / relations.size();
        double del = config.delta() / relations.size();

        for (String[] r : relations) {
            String child = r[0], col = r[1], parent = r[2];
            long parentRows = countRows(conn, parent);
            long[] degrees  = sortedDegrees(conn, child, col);
            DpRelease.ForeignKeyDegree fk =
                    release(child, col, parent, degrees, parentRows, eps, del, noise);
            if (fk != null) {
                out.put(child + "." + col, fk);
                LOG.info(String.format("[FkDegree] %s.%s -> %s: top 1%% of parents own %.1f%% of rows",
                        child, col, parent, 100 * shareAt(fk, 1e-2)));
            }
        }
        return out;
    }

    /**
     * Pure release step (testable without a database).
     *
     * @param degreesDesc degrees of parents that have at least one child, sorted descending
     * @param parentRows  total number of parent rows (public)
     */
    static DpRelease.ForeignKeyDegree release(String child, String col, String parent,
                                              long[] degreesDesc, long parentRows,
                                              double eps, double del, Random noise) {
        if (parentRows <= 0) return null;

        // Fixed checkpoints k_i = round(q_i · N), deduplicated (tiny parent tables).
        List<Double> fractions = new ArrayList<>();
        List<Long>   ranks     = new ArrayList<>();
        long prev = 0;
        for (double q : RANK_FRACTIONS) {
            long k = Math.min(parentRows, Math.max(1, Math.round(q * parentRows)));
            if (k > prev) {
                ranks.add(k);
                fractions.add(q == 1.0 ? 1.0 : (double) k / parentRows);
                prev = k;
            }
        }
        int m = ranks.size();

        // Exact cumulative sums of the sorted degree sequence at each checkpoint.
        double[] cum = new double[m];
        long running = 0;
        int idx = 0;
        for (int i = 0; i < m; i++) {
            long k = ranks.get(i);
            while (idx < degreesDesc.length && idx < k) running += degreesDesc[idx++];
            cum[i] = running;
        }

        double sigma = escuelaing.edu.co.infrastructure.capture.DpReleaseBuilder
                .gaussianSigma(Math.sqrt(m), eps, del);
        for (int i = 0; i < m; i++) cum[i] += noise.nextGaussian() * sigma;

        List<Double> shares = postProcess(cum, ranks);
        return DpRelease.ForeignKeyDegree.builder()
                .childTable(child).column(col).parentTable(parent)
                .rankFractions(Collections.unmodifiableList(fractions))
                .cumulativeShare(shares)
                .sigma(sigma)
                .build();
    }

    /**
     * Turns noisy cumulative counts into a valid concentration curve: per-segment average
     * degrees clamped at zero and made non-increasing, then normalised so the last share is 1.
     * Falls back to a uniform curve if nothing survives.
     */
    static List<Double> postProcess(double[] noisyCum, List<Long> ranks) {
        int m = noisyCum.length;
        double[] width = new double[m];
        double[] avg   = new double[m];
        long prevK = 0;
        double prevC = 0;
        for (int i = 0; i < m; i++) {
            width[i] = ranks.get(i) - prevK;
            avg[i]   = Math.max(0.0, (noisyCum[i] - prevC) / width[i]);
            prevK = ranks.get(i);
            prevC = noisyCum[i];
        }
        double[] fitted = nonIncreasing(avg, width);

        double total = 0;
        for (int i = 0; i < m; i++) total += fitted[i] * width[i];
        List<Double> shares = new ArrayList<>(m);
        double acc = 0;
        long lastK = ranks.get(m - 1);
        for (int i = 0; i < m; i++) {
            acc += fitted[i] * width[i];
            shares.add(total > 0 ? Math.min(1.0, acc / total) : (double) ranks.get(i) / lastK);
        }
        shares.set(m - 1, 1.0);
        return Collections.unmodifiableList(shares);
    }

    /** Weighted pool-adjacent-violators for a non-increasing fit. */
    static double[] nonIncreasing(double[] y, double[] w) {
        int n = y.length;
        double[] val = new double[n];
        double[] wt  = new double[n];
        int[]    len = new int[n];
        int top = 0;
        for (int i = 0; i < n; i++) {
            val[top] = y[i]; wt[top] = w[i]; len[top] = 1;
            while (top > 0 && val[top - 1] < val[top]) {
                double ww = wt[top - 1] + wt[top];
                val[top - 1] = (val[top - 1] * wt[top - 1] + val[top] * wt[top]) / ww;
                wt[top - 1]  = ww;
                len[top - 1] += len[top];
                top--;
            }
            top++;
        }
        double[] out = new double[n];
        int pos = 0;
        for (int b = 0; b < top; b++) {
            for (int j = 0; j < len[b]; j++) out[pos++] = val[b];
        }
        return out;
    }

    /** Share of child rows owned by the top {@code q} fraction of parents (linear interpolation). */
    public static double shareAt(DpRelease.ForeignKeyDegree fk, double q) {
        List<Double> f = fk.getRankFractions();
        List<Double> s = fk.getCumulativeShare();
        double pf = 0, ps = 0;
        for (int i = 0; i < f.size(); i++) {
            if (q <= f.get(i)) {
                double t = (q - pf) / (f.get(i) - pf);
                return ps + t * (s.get(i) - ps);
            }
            pf = f.get(i);
            ps = s.get(i);
        }
        return 1.0;
    }

    // ------------------------------------------------------------------ database access

    private static List<String[]> discoverForeignKeys(Connection conn) throws SQLException {
        String sql = """
                SELECT kcu.table_name  AS child_table,
                       kcu.column_name AS fk_column,
                       ccu.table_name  AS parent_table
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema    = kcu.table_schema
                JOIN information_schema.referential_constraints rc
                  ON tc.constraint_name   = rc.constraint_name
                 AND tc.constraint_schema = rc.constraint_schema
                JOIN information_schema.constraint_column_usage ccu
                  ON rc.unique_constraint_name   = ccu.constraint_name
                 AND rc.unique_constraint_schema = ccu.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_schema    = 'public'
                ORDER BY 1, 2
                """;
        List<String[]> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(new String[]{rs.getString(1), rs.getString(2), rs.getString(3)});
            }
        }
        return out;
    }

    private static long countRows(Connection conn, String table) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + quote(table))) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    private static long[] sortedDegrees(Connection conn, String child, String col) throws SQLException {
        String sql = "SELECT COUNT(*) AS c FROM " + quote(child)
                + " WHERE " + quote(col) + " IS NOT NULL GROUP BY " + quote(col) + " ORDER BY c DESC";
        List<Long> list = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) list.add(rs.getLong(1));
        }
        long[] out = new long[list.size()];
        for (int i = 0; i < out.length; i++) out[i] = list.get(i);
        return out;
    }

    /** Identifiers come from information_schema; quoted defensively. */
    private static String quote(String ident) {
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }
}
