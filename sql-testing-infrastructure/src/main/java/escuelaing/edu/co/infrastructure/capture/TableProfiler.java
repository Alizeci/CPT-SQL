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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 * Table-level differentially-private release of the business columns of the capture
 * database.
 *
 * <p>The row-level release ({@link DpReleaseBuilder#build}) only sees the rows the
 * application happens to read during the capture window. Those rows are biased towards
 * hot records (e.g. the simulator mostly reads products 1–500, all of one category),
 * so the mirror inherited that bias. This profiler computes the same statistics over
 * the <em>whole</em> tables instead:</p>
 * <ul>
 *   <li><b>Numeric:</b> every column that has declared bounds ({@code CPT_DP_BOUNDS}) and a
 *       numeric type, in any table: Gaussian mechanism on the clipped mean and second moment
 *       (sensitivity {@code (hi−lo)/n} and {@code (max²−min²)/n}, {@code n} = table rows,
 *       treated as public).</li>
 *   <li><b>Categorical:</b> only the columns declared in {@code CPT_DP_CATEGORICAL}
 *       ({@code table.column}): thresholded Gaussian histogram (L2 sensitivity √2), so rare
 *       values are never published.</li>
 * </ul>
 *
 * <p>Neighbouring datasets differ in one row of one table (record-level DP). The budget
 * {@code ε_numeric} is split evenly across the {@code 2·k} numeric releases and
 * {@code ε_categorical} across the declared categorical columns; {@code δ} across all.</p>
 */
public final class TableProfiler {

    private static final Logger LOG = Logger.getLogger(TableProfiler.class.getName());

    static final String MECHANISM =
            "table-level release over the whole capture tables — numeric: Gaussian mechanism on clipped "
          + "mean and second moment (declared bounds); categorical (declared columns): thresholded "
          + "Gaussian histogram; identifiers: never released";

    private TableProfiler() {}

    public static DpRelease profile(Connection conn, DpConfig config) throws SQLException {
        return profile(conn, config, new SecureRandom());
    }

    static DpRelease profile(Connection conn, DpConfig config, Random noise) throws SQLException {
        DpReleaseBuilder builder = new DpReleaseBuilder(config, noise);

        // Numeric columns with declared bounds, located in any public table.
        List<String[]> numericCols = new ArrayList<>();       // [table, column]
        try (PreparedStatement ps = conn.prepareStatement("""
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND data_type IN ('integer','bigint','smallint','numeric','real','double precision')
                ORDER BY table_name, column_name
                """);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String col = rs.getString(2).toLowerCase();
                if (config.bounds().containsKey(col)) numericCols.add(new String[]{rs.getString(1), col});
            }
        }
        List<String> categoricalCols = config.categoricalColumns();

        int numericReleases = 2 * numericCols.size();
        int totalReleases   = Math.max(1, numericReleases + categoricalCols.size());
        double del = config.delta() / totalReleases;

        Map<String, DpRelease.NumericColumn> numeric = new LinkedHashMap<>();
        long sourceRows = 0;
        if (!numericCols.isEmpty()) {
            double eps = config.epsilonNumeric() / numericReleases;
            for (String[] tc : numericCols) {
                double[] b = config.bounds().get(tc[1]);
                String c = quote(tc[1]);
                String clip = "LEAST(GREATEST(" + c + "::double precision, " + b[0] + "), " + b[1] + ")";
                String sql = "SELECT COUNT(" + c + "), COALESCE(SUM(" + clip + "),0), COALESCE(SUM("
                        + clip + " * " + clip + "),0) FROM " + quote(tc[0]);
                try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                    rs.next();
                    long n = rs.getLong(1);
                    if (n == 0) continue;
                    sourceRows = Math.max(sourceRows, n);
                    numeric.put(tc[1], builder.releaseNumericMoments(n, rs.getDouble(2), rs.getDouble(3),
                            b[0], b[1], eps, del));
                }
            }
        }

        Map<String, Map<String, Double>> categorical = new LinkedHashMap<>();
        List<String> suppressed = new ArrayList<>();
        if (!categoricalCols.isEmpty()) {
            double eps = config.epsilonCategorical() / categoricalCols.size();
            double sigma = DpReleaseBuilder.gaussianSigma(Math.sqrt(2.0), eps, del);
            double threshold = 1.0 + sigma * Math.sqrt(2.0 * Math.log(1.0 / del));
            for (String tc : categoricalCols) {
                String[] parts = tc.split("\\.");
                String sql = "SELECT " + quote(parts[1]) + "::text, COUNT(*) FROM " + quote(parts[0])
                        + " WHERE " + quote(parts[1]) + " IS NOT NULL GROUP BY 1";
                Map<String, Long> counts = new TreeMap<>();
                try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                    while (rs.next()) counts.put(rs.getString(1), rs.getLong(2));
                }
                Map<String, Double> released = builder.releaseHistogram(counts, sigma, threshold);
                if (released.isEmpty()) suppressed.add(parts[1]);
                else categorical.put(parts[1], released);
            }
        }

        LOG.info("[TableProfiler] Released numeric " + numeric.keySet()
                + ", categorical " + categorical.keySet() + ", suppressed " + suppressed);

        return DpRelease.builder()
                .mechanism(MECHANISM)
                .epsilonNumeric(numericCols.isEmpty() ? 0.0 : config.epsilonNumeric())
                .epsilonCategorical(categoricalCols.isEmpty() ? 0.0 : config.epsilonCategorical())
                .delta(config.delta())
                .sourceRows(sourceRows)
                .numeric(numeric)
                .categorical(categorical)
                .suppressedColumns(Collections.unmodifiableList(new ArrayList<>(new TreeSet<>(suppressed))))
                .build();
    }

    private static String quote(String ident) {
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }
}
