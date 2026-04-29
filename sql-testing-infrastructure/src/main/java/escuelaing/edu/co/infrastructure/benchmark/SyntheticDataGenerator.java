package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.domain.model.LoadProfile;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

/**
 * Populates the mirror database with differentially-private synthetic data.
 *
 * <p>Columns are split into two categories with independent privacy budgets:
 * <b>Feature columns</b> (numeric business values such as price or rating) receive
 * Gaussian noise calibrated from production statistics, preserving the distributions
 * that affect query selectivity. <b>UII columns</b> (any {@code *_id}, {@code user_id},
 * {@code customer_id}) are replaced by pseudonyms in the exclusive range [10001, 99999]
 * so real identifiers never enter the mirror database.</p>
 *
 * <p>Because Feature and UII mechanisms operate on disjoint column sets, the total
 * privacy budget is simply the sum: ε_total = ε_ft + ε_uii = 1.0. The statistics
 * used for calibration come from {@code sanitizedRealData} captured at sampling
 * rate {@code q} (default 0.1), so the effective budget with respect to the full
 * production dataset is ε_eff = q × ε_ft = 0.06.</p>
 *
 * <p>Real sanitized rows are seeded first (10 %), then synthetic rows fill the
 * remaining volume (90 %). All random decisions use a single seeded
 * {@link java.util.Random}, making two runs with the same seed byte-identical.</p>
 *
 * <p>Tables are discovered via {@code INFORMATION_SCHEMA.tables} — the generator
 * is schema-agnostic and works with any PostgreSQL schema without modification.
 * Foreign key constraints are discovered from {@code INFORMATION_SCHEMA} and used
 * to sort tables topologically (parents before children) and to generate FK column
 * values that reference existing rows in the parent table.</p>
 *
 * <pre>
 * loadtest.synthetic.rowsPerTable=500
 * loadtest.synthetic.batchSize=200
 * loadtest.synthetic.seed=42
 * loadtest.synthetic.subsamplingRate=0.1   # must match SamplingFilter rate in Phase 2
 * </pre>
 */
@Component
public class SyntheticDataGenerator {

    private static final Logger LOG = Logger.getLogger(SyntheticDataGenerator.class.getName());

    /** Privacy budget for Feature columns. Noise = sensitivity / ε_ft. */
    static final double EPSILON_FT  = 0.6;

    /** Privacy budget for UII columns (identifiers replaced by pseudonyms). */
    static final double EPSILON_UII = 0.4;

    /** Total budget = ε_ft + ε_uii; valid because Feature and UII column sets are disjoint. */
    static final double EPSILON_TOTAL = EPSILON_FT + EPSILON_UII; // 1.0

    /** Pseudonym range for UII columns — does not overlap with production IDs (1–10000). */
    private static final int UII_MIN = 10_001;
    private static final int UII_MAX = 99_999;

    /** Max acceptable p95 error (%) between synthetic and production latency in fidelity validation. */
    static final double LATENCY_ERROR_THRESHOLD_PCT = 10.0;

    /** Max acceptable cardinality error (%) for write queries in fidelity validation. */
    static final double CARDINALITY_ERROR_THRESHOLD_PCT = 5.0;

    /** Cardinality ratio above which a string column is considered high-cardinality (likely PII). */
    private static final double STRING_CARDINALITY_THRESHOLD = 0.5;

    @Value("${loadtest.synthetic.rowsPerTable:500}")
    private int rowsPerTable;

    @Value("${loadtest.synthetic.batchSize:200}")
    private int batchSize;

    @Value("${loadtest.synthetic.seed:42}")
    private long seed;

    /**
     * Fraction of production queries sampled by Phase 2 (default 0.1 = 10 %).
     * Must match {@code loadtest.capture.samplingRate}.
     * Used to compute ε_eff = subsamplingRate × ε_ft — the effective privacy budget
     * with respect to the full production dataset.
     */
    @Value("${loadtest.synthetic.subsamplingRate:0.1}")
    private double subsamplingRate;

    /**
     * Comma-separated list of column names to exclude from the string parameter pool.
     * Configure in {@code application.properties} (keep out of version control).
     * Columns not listed here are filtered automatically by cardinality.
     */
    @Value("${loadtest.synthetic.sensitiveColumns:}")
    private String sensitiveColumnsRaw;

    private Random rng = new Random(42);

    /** Rows inserted by the last {@link #populate} call. */
    private long lastRowsGenerated = 0;

    /** Tables populated by the last {@link #populate} call. */
    private int lastTablesPopulated = 0;

    /**
     * Cache of primary key values per table, populated lazily during {@link #populate}.
     * Cleared at the start of each populate call so stale IDs from a previous mirror
     * run are never reused.
     */
    private final Map<String, List<Integer>> pkCache = new HashMap<>();

    /** Reseeds {@code rng} with the Spring-injected {@code seed}. */
    @PostConstruct
    void initRng() {
        this.rng = new Random(seed);
    }

    public long getSeed() { return seed; }

    public void setSeed(long seed) {
        this.seed = seed;
        this.rng  = new Random(seed);
    }

    /**
     * Returns the effective privacy budget after applying privacy amplification by subsampling.
     *
     * <p>The Phase 2 SamplingFilter captures only a {@code subsamplingRate} fraction
     * of production queries. Because the Gaussian mechanism is applied to this
     * q-subsample, the formal guarantee tightens from ε_ft to q·ε_ft with respect
     * to the full production dataset — at no additional noise cost.</p>
     *
     * @return effective ε for Feature columns = subsamplingRate × EPSILON_FT
     */
    public double getEffectiveEpsilon() {
        return subsamplingRate * EPSILON_FT;
    }

    public long getLastRowsGenerated()  { return lastRowsGenerated; }
    public int  getLastTablesPopulated() { return lastTablesPopulated; }

    // Column classification: Feature vs UII

    /**
     * Column categories for differentially-private data generation.
     *
     * <ul>
     *   <li>{@code IDENTIFIER} — any column named {@code id} or ending in {@code _id}.
     *       Replaced by a pseudonym in the exclusive range [{@code UII_MIN}, {@code UII_MAX}]
     *       so real identifiers never enter the mirror database.</li>
     *   <li>{@code FEATURE} — any other column (numeric business values, strings, booleans).
     *       Generated preserving the statistical distribution from production.</li>
     * </ul>
     */
    public enum ColumnCategory { FEATURE, IDENTIFIER }

    /**
     * Classifies a column as IDENTIFIER or FEATURE based on its name.
     * Any column named {@code id} or ending with {@code _id} is treated as an identifier
     * and receives a pseudonym — regardless of the domain it represents.
     */
    public ColumnCategory classifyColumn(String tableName, String columnName) {
        String col = columnName.toLowerCase();
        if (col.equals("id") || col.endsWith("_id")) {
            return ColumnCategory.IDENTIFIER;
        }
        return ColumnCategory.FEATURE;
    }

    /**
     * Generates an integer pseudonym in [{@code UII_MIN}, {@code UII_MAX}]
     * that never collides with real production IDs (typical range 1–10000).
     */
    public int generateUiiPseudonym() {
        return UII_MIN + rng.nextInt(UII_MAX - UII_MIN + 1);
    }

    // Checksum for reproducibility verification

    /**
     * Computes a CRC32 checksum over the current contents of {@code tableName}.
     * Used to verify that two generations with the same seed produce byte-identical data.
     *
     * @param conn      open connection to the mirror database
     * @param tableName table to checksum
     * @return CRC32 checksum over all rows in the table
     */
    public long computeChecksum(Connection conn, String tableName) throws SQLException {
        CRC32 crc = new CRC32();
        String sql = "SELECT * FROM " + tableName + " ORDER BY 1";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= cols; i++) {
                    Object v = rs.getObject(i);
                    if (v != null) crc.update(v.toString().getBytes());
                }
            }
        }
        return crc.getValue();
    }

    // Public API

    /**
     * Populates the mirror database with synthetic data derived from {@code profile}.
     * Tables are discovered automatically via {@code INFORMATION_SCHEMA} — no schema
     * configuration is required.
     *
     * <p>Foreign key constraints are discovered and used to:</p>
     * <ol>
     *   <li>Sort tables topologically so parent tables are always populated before their
     *       dependents, avoiding FK violations during insertion.</li>
     *   <li>Generate FK column values by selecting a random existing ID from the parent
     *       table instead of an arbitrary pseudonym that would violate the constraint.</li>
     * </ol>
     *
     * @param conn    open connection to the mirror database
     * @param profile Phase 2 load profile used to calibrate distributions
     */
    public void populate(Connection conn, LoadProfile profile) throws SQLException {
        LOG.info("[SyntheticData] Starting mirror database population...");
        pkCache.clear();
        conn.setAutoCommit(false);

        List<String> tables = discoverUserTables(conn);
        lastTablesPopulated = tables.size();

        Map<String, Map<String, String>> fkConstraints = discoverFkConstraints(conn);
        List<String> sortedTables = topologicalSort(tables, fkConstraints);

        Map<String, double[]> dpStats = buildDpStats(profile);
        insertRealSanitizedData(conn, profile, sortedTables, dpStats, fkConstraints);

        for (String table : sortedTables) {
            Savepoint sp = conn.setSavepoint();
            try {
                List<ColumnMeta> cols = describeTable(conn, table);
                if (cols.isEmpty()) continue;
                Map<String, String> fkCols = fkConstraints.getOrDefault(table, Collections.emptyMap());
                insertRows(conn, table, cols, rowsPerTable, dpStats, fkCols);
            } catch (SQLException e) {
                LOG.warning("[SyntheticData] Could not populate '" + table + "': " + e.getMessage());
                conn.rollback(sp);
            }
        }

        conn.commit();
        conn.setAutoCommit(true);
        lastRowsGenerated = countTotalRows(conn, tables);
        LOG.info("[SyntheticData] Mirror database populated: " + tables.size()
                + " tables, " + lastRowsGenerated + " rows.");
    }

    // Schema discovery

    List<String> discoverUserTables(Connection conn) throws SQLException {
        List<String> tables = new ArrayList<>();
        String sql = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type   = 'BASE TABLE'
                ORDER BY table_name
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) tables.add(rs.getString(1));
        }
        LOG.info("[SyntheticData] Tables discovered: " + tables);
        return tables;
    }

    /**
     * Discovers all foreign key constraints in the public schema.
     *
     * @return map of child_table → (fk_column → parent_table)
     */
    Map<String, Map<String, String>> discoverFkConstraints(Connection conn) throws SQLException {
        Map<String, Map<String, String>> result = new HashMap<>();
        String sql = """
                SELECT
                    kcu.table_name  AS child_table,
                    kcu.column_name AS fk_column,
                    ccu.table_name  AS parent_table
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                   AND tc.table_schema    = kcu.table_schema
                JOIN information_schema.referential_constraints rc
                    ON tc.constraint_name    = rc.constraint_name
                   AND tc.constraint_schema  = rc.constraint_schema
                JOIN information_schema.constraint_column_usage ccu
                    ON rc.unique_constraint_name    = ccu.constraint_name
                   AND rc.unique_constraint_schema  = ccu.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_schema    = 'public'
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String child  = rs.getString("child_table");
                String col    = rs.getString("fk_column");
                String parent = rs.getString("parent_table");
                result.computeIfAbsent(child, k -> new HashMap<>()).put(col, parent);
            }
        }
        return result;
    }

    /**
     * Sorts {@code tables} in topological order so that each table appears after all
     * tables it depends on via foreign keys. Uses an iterative algorithm that repeatedly
     * selects tables whose parents are already placed. If a cycle is detected (no
     * progress possible), the remaining tables are appended as-is.
     */
    List<String> topologicalSort(List<String> tables,
                                  Map<String, Map<String, String>> fkMap) {
        List<String> sorted    = new ArrayList<>();
        Set<String>  remaining = new LinkedHashSet<>(tables);

        while (!remaining.isEmpty()) {
            boolean progress = false;
            Iterator<String> it = remaining.iterator();
            while (it.hasNext()) {
                String table   = it.next();
                Set<String> parents = new HashSet<>(
                        fkMap.getOrDefault(table, Collections.emptyMap()).values());
                parents.retainAll(remaining); // only unsorted parents block this table
                if (parents.isEmpty()) {
                    sorted.add(table);
                    it.remove();
                    progress = true;
                }
            }
            if (!progress) {
                // Circular FK dependency — append remaining to avoid infinite loop
                sorted.addAll(remaining);
                break;
            }
        }
        return sorted;
    }

    private List<ColumnMeta> describeTable(Connection conn, String table) throws SQLException {
        List<ColumnMeta> cols = new ArrayList<>();
        String sql = """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = ?
                ORDER BY ordinal_position
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cols.add(new ColumnMeta(
                            rs.getString("column_name"),
                            rs.getString("data_type"),
                            "YES".equals(rs.getString("is_nullable")),
                            rs.getString("column_default")));
                }
            }
        }
        return cols;
    }

    // Row insertion

    /**
     * Seeds the mirror database with real sanitized rows from the load profile,
     * then fills the remaining volume with synthetic rows.
     *
     * <p>FK columns are resolved against the already-populated parent table so that
     * referential integrity is maintained without exposing real production identifiers.</p>
     */
    void insertRealSanitizedData(Connection conn, LoadProfile profile,
                                  List<String> tables,
                                  Map<String, double[]> dpStats,
                                  Map<String, Map<String, String>> fkConstraints) throws SQLException {
        if (profile == null || profile.getQueries() == null) return;

        Map<String, List<ColumnMeta>> tableSchema = new HashMap<>();
        for (String table : tables) {
            tableSchema.put(table, describeTable(conn, table));
        }

        int totalInserted = 0;
        for (LoadProfile.QueryStats stats : profile.getQueries().values()) {
            if (stats.getSanitizedRealData() == null) continue;
            for (Map<String, Object> row : stats.getSanitizedRealData()) {
                if (row == null || row.isEmpty()) continue;
                String target = findBestMatchingTable(row.keySet(), tableSchema);
                if (target == null) continue;
                Map<String, String> fkCols = fkConstraints.getOrDefault(target, Collections.emptyMap());
                Savepoint sp = conn.setSavepoint();
                try {
                    insertRealRow(conn, target, row, tableSchema.get(target), dpStats, fkCols);
                    totalInserted++;
                } catch (SQLException e) {
                    LOG.warning("[SyntheticData] Real row discarded: " + e.getMessage());
                    conn.rollback(sp);
                }
            }
        }
        LOG.info("[SyntheticData] Real rows seeded: " + totalInserted);
    }

    private String findBestMatchingTable(Set<String> rowKeys,
                                          Map<String, List<ColumnMeta>> tableSchema) {
        String bestTable = null;
        long bestMatch = 0;
        for (Map.Entry<String, List<ColumnMeta>> entry : tableSchema.entrySet()) {
            Set<String> tableColNames = entry.getValue().stream()
                    .map(c -> c.name).collect(Collectors.toSet());
            long matches = rowKeys.stream().filter(tableColNames::contains).count();
            if (matches > bestMatch) {
                bestMatch = matches;
                bestTable = entry.getKey();
            }
        }
        return bestMatch > 0 ? bestTable : null;
    }

    private void insertRealRow(Connection conn, String table, Map<String, Object> row,
                                List<ColumnMeta> tableCols,
                                Map<String, double[]> dpStats,
                                Map<String, String> fkColumns) throws SQLException {
        List<ColumnMeta> insertable = tableCols.stream()
                .filter(c -> c.defaultValue == null || c.defaultValue.isEmpty())
                .toList();
        if (insertable.isEmpty()) return;

        String colNames     = insertable.stream().map(c -> c.name).collect(Collectors.joining(", "));
        String placeholders = insertable.stream().map(c -> "?").collect(Collectors.joining(", "));
        String sql = "INSERT INTO " + table + " (" + colNames + ") VALUES (" + placeholders
                + ") ON CONFLICT DO NOTHING";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < insertable.size(); i++) {
                ColumnMeta col = insertable.get(i);
                ColumnCategory cat = classifyColumn(table, col.name);
                if (cat == ColumnCategory.IDENTIFIER) {
                    String parentTable = fkColumns.get(col.name);
                    Integer existingId = parentTable != null ? pickExistingPk(conn, parentTable) : null;
                    ps.setInt(i + 1, existingId != null ? existingId : generateUiiPseudonym());
                } else if (row.containsKey(col.name)) {
                    ps.setObject(i + 1, row.get(col.name));
                } else {
                    setColumnValue(ps, i + 1, col, dpStats, conn, fkColumns);
                }
            }
            ps.executeUpdate();
        }
    }

    private void insertRows(Connection conn, String table,
                             List<ColumnMeta> cols, int rows,
                             Map<String, double[]> dpStats,
                             Map<String, String> fkColumns) throws SQLException {
        List<ColumnMeta> insertable = cols.stream()
                .filter(c -> c.defaultValue == null || c.defaultValue.isEmpty())
                .toList();
        if (insertable.isEmpty()) return;

        String colNames     = insertable.stream().map(c -> c.name).collect(Collectors.joining(", "));
        String placeholders = insertable.stream().map(c -> "?").collect(Collectors.joining(", "));
        String sql = "INSERT INTO " + table + " (" + colNames + ") VALUES (" + placeholders
                + ") ON CONFLICT DO NOTHING";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < insertable.size(); j++) {
                    setColumnValue(ps, j + 1, insertable.get(j), dpStats, conn, fkColumns);
                }
                ps.addBatch();
                if ((i + 1) % batchSize == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }
    }

    private void setColumnValue(PreparedStatement ps, int idx, ColumnMeta col,
                                 Map<String, double[]> dpStats,
                                 Connection conn, Map<String, String> fkColumns) throws SQLException {
        ColumnCategory category = classifyColumn("", col.name);
        if (category == ColumnCategory.IDENTIFIER) {
            String parentTable = fkColumns != null ? fkColumns.get(col.name) : null;
            if (parentTable != null) {
                Integer existingId = pickExistingPk(conn, parentTable);
                ps.setInt(idx, existingId != null ? existingId : generateUiiPseudonym());
            } else {
                ps.setInt(idx, generateUiiPseudonym());
            }
            return;
        }
        double[] stats = dpStats.get(col.name);
        double mean  = stats != null ? stats[0] : defaultMeanFor(col.dataType);
        double sigma = stats != null ? stats[1] : defaultSigmaFor(col.dataType);
        switch (col.dataType.toLowerCase()) {
            case "integer", "int", "int4", "int8", "bigint", "smallint" ->
                    ps.setInt(idx, (int) clamp(gaussianNoise(mean, sigma), 1, 10_000));
            case "numeric", "decimal", "real", "double precision", "float8" ->
                    ps.setBigDecimal(idx, BigDecimal.valueOf(
                            Math.round(clamp(gaussianNoise(mean, sigma), 0.01, 999_999.99) * 100) / 100.0));
            case "boolean", "bool" ->
                    ps.setBoolean(idx, rng.nextBoolean());
            case "timestamp", "timestamp without time zone", "timestamp with time zone" ->
                    ps.setTimestamp(idx, java.sql.Timestamp.from(java.time.Instant.now()
                            .minusSeconds(rng.nextInt(86_400 * 30))));
            case "char", "character", "character varying", "varchar", "text" ->
                    ps.setString(idx, randomString(6, 20));
            default -> ps.setString(idx, randomString(4, 10));
        }
    }

    /**
     * Returns a random primary key value from {@code tableName}, caching up to 500
     * IDs per table to avoid repeated queries. Returns {@code null} if the table is empty.
     *
     * <p>The cache is keyed by table name and populated lazily on first access.
     * Because tables are processed in topological order, the parent table is always
     * fully populated before any of its children query this cache.</p>
     */
    private Integer pickExistingPk(Connection conn, String tableName) throws SQLException {
        if (!pkCache.containsKey(tableName)) {
            List<Integer> ids = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id FROM " + tableName + " ORDER BY random() LIMIT 500");
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) ids.add(rs.getInt(1));
            }
            pkCache.put(tableName, ids);
        }
        List<Integer> ids = pkCache.get(tableName);
        return ids.isEmpty() ? null : ids.get(rng.nextInt(ids.size()));
    }

    private double defaultMeanFor(String dataType) {
        return switch (dataType.toLowerCase()) {
            case "numeric", "decimal", "real", "double precision", "float8" -> 50.0;
            default -> 100.0;
        };
    }

    private double defaultSigmaFor(String dataType) {
        return switch (dataType.toLowerCase()) {
            case "numeric", "decimal", "real", "double precision", "float8" -> 30.0;
            default -> 50.0;
        };
    }

    // String parameter pool for QueryExecutor

    /**
     * Builds a pool of string values extracted from {@code sanitizedRealData} in the profile.
     * The pool is used by {@link QueryExecutor} to bind VARCHAR parameters with realistic,
     * distribution-preserving values instead of hardcoded domain strings.
     *
     * <p>Values are collected with repetition so that random sampling from the pool
     * naturally reproduces the production frequency distribution (e.g. if "DELIVERED"
     * appears in 70 % of rows, it will be sampled ~70 % of the time).</p>
     *
     * <p>Two filters are applied in order:</p>
     * <ol>
     *   <li><b>Explicit config:</b> columns listed in {@code loadtest.synthetic.sensitiveColumns}
     *       are always excluded.</li>
     *   <li><b>Cardinality fallback:</b> columns where
     *       {@code uniqueValues / totalValues > 0.5} are excluded as likely PII.</li>
     * </ol>
     *
     * @return immutable list of safe string values; empty if no suitable values found
     */
    public List<String> buildStringPool(LoadProfile profile) {
        if (profile == null || profile.getQueries() == null) return Collections.emptyList();

        Set<String> sensitive = parseSensitiveColumns();

        Map<String, List<String>> columnValues = new HashMap<>();
        for (LoadProfile.QueryStats stats : profile.getQueries().values()) {
            if (stats.getSanitizedRealData() == null) continue;
            for (Map<String, Object> row : stats.getSanitizedRealData()) {
                if (row == null) continue;
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    if (entry.getValue() instanceof String s && !s.isBlank()) {
                        columnValues.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(s);
                    }
                }
            }
        }

        List<String> pool = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : columnValues.entrySet()) {
            String col = entry.getKey();
            List<String> values = entry.getValue();

            if (sensitive.contains(col)) {
                LOG.fine("[SyntheticData] String column '" + col + "' excluded (sensitive config).");
                continue;
            }
            long uniqueCount = values.stream().distinct().count();
            double cardinality = (double) uniqueCount / values.size();
            if (cardinality > STRING_CARDINALITY_THRESHOLD) {
                LOG.fine("[SyntheticData] String column '" + col + "' excluded (cardinality="
                        + String.format("%.2f", cardinality) + ").");
                continue;
            }
            pool.addAll(values);
        }

        LOG.info("[SyntheticData] String pool built: " + pool.size() + " values from "
                + columnValues.size() + " string column(s) in profile.");
        return Collections.unmodifiableList(pool);
    }

    private Set<String> parseSensitiveColumns() {
        if (sensitiveColumnsRaw == null || sensitiveColumnsRaw.isBlank()) return Collections.emptySet();
        return java.util.Arrays.stream(sensitiveColumnsRaw.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }

    // DP calibration from profile statistics

    /**
     * Extracts per-column DP-calibrated statistics from sanitizedRealData in the profile.
     *
     * <p>Collects numeric values by column name across all queries, computes mean and
     * standard deviation, then applies the Gaussian mechanism with ε={@link #EPSILON_FT}.</p>
     *
     * <p>The sanitizedRealData was collected at {@code subsamplingRate} (default 10 %).
     * Because the mechanism is applied to a q-subsample of the full population,
     * the formal guarantee with respect to the full production dataset is
     * ε_eff = q × ε_ft (logged below), not ε_ft — at no additional noise cost.</p>
     *
     * @return map of column name → [dpMean, dpStd]; empty if no samples available
     */
    Map<String, double[]> buildDpStats(LoadProfile profile) {
        if (profile == null || profile.getQueries() == null) return Collections.emptyMap();

        Map<String, List<Double>> columnValues = new HashMap<>();
        for (LoadProfile.QueryStats stats : profile.getQueries().values()) {
            if (stats.getSanitizedRealData() == null) continue;
            for (Map<String, Object> row : stats.getSanitizedRealData()) {
                if (row == null) continue;
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    if (entry.getValue() instanceof Number n) {
                        columnValues.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                                    .add(n.doubleValue());
                    }
                }
            }
        }

        // Privacy amplification by subsampling: ε_eff = q × ε_ft.
        // The noise level stays the same; the formal guarantee tightens because the
        // mechanism was applied to a q-subsample of the full production dataset.
        double effectiveEpsilon = subsamplingRate * EPSILON_FT;

        Map<String, double[]> dpStats = new HashMap<>();
        for (Map.Entry<String, List<Double>> entry : columnValues.entrySet()) {
            List<Double> values = entry.getValue().stream().filter(v -> v >= 0).toList();
            if (values.size() < 2) continue;
            double m = mean(values);
            double s = std(values, m);
            int n = values.size();
            double sensitivity = Math.max(m, 1.0) / n;
            double dpMean = clamp(gaussianNoise(m, sensitivity), 0.0, m * 3 + 1);
            double dpStd  = Math.max(0.1, s);
            dpStats.put(entry.getKey(), new double[]{dpMean, dpStd});
        }

        if (!dpStats.isEmpty()) {
            LOG.info(String.format(
                    "[SyntheticData] DP stats built for columns %s " +
                    "(ε_ft=%.2f, subsamplingRate=%.2f → ε_eff=%.4f w.r.t. full dataset)",
                    dpStats.keySet(), EPSILON_FT, subsamplingRate, effectiveEpsilon));
        }
        return dpStats;
    }

    // Utilities

    private long countTotalRows(Connection conn, List<String> tables) {
        long total = 0;
        for (String table : tables) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + table);
                 ResultSet rs = ps.executeQuery()) {
                if (rs.next()) total += rs.getLong(1);
            } catch (SQLException ignored) {}
        }
        return total;
    }

    /**
     * Applies the Gaussian mechanism with budget ε_ft to a Feature column.
     * Sensitivity is scaled by (1 / ε_ft): more noise at higher privacy (lower ε),
     * less noise at lower privacy (higher ε).
     */
    double gaussianNoise(double base, double sigma) {
        return base + rng.nextGaussian() * (sigma / EPSILON_FT);
    }

    private static final String ALPHANUM =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    private String randomString(int minLen, int maxLen) {
        int len = minLen + (minLen == maxLen ? 0 : rng.nextInt(maxLen - minLen + 1));
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) sb.append(ALPHANUM.charAt(rng.nextInt(ALPHANUM.length())));
        return sb.toString();
    }

    private double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }

    private record ColumnMeta(String name, String dataType, boolean nullable, String defaultValue) {}

    private double mean(List<Double> values) {
        if (values.isEmpty()) return 0.0;
        return values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }

    private double std(List<Double> values, double mean) {
        if (values.size() < 2) return 1.0;
        double variance = values.stream()
                .mapToDouble(v -> (v - mean) * (v - mean))
                .average().orElse(1.0);
        return Math.sqrt(variance);
    }
}
