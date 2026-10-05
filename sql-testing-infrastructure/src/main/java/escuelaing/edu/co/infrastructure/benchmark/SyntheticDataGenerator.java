package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.domain.model.DpRelease;
import escuelaing.edu.co.domain.model.LoadProfile;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import escuelaing.edu.co.infrastructure.dialect.DatabaseDialect;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
 * Populates the mirror database with synthetic data derived only from the
 * differentially-private release produced in Phase 2 ({@link DpRelease}).
 *
 * <p><b>What reaches the mirror:</b> numeric business columns are sampled from
 * N(mean, std) using the released (noisy) mean and standard deviation, clamped to the
 * declared bounds; categorical columns are sampled from the released noisy histogram;
 * identifier columns receive synthetic pseudonyms or references to synthetic parent rows.
 * Because the generator only post-processes the release, it consumes no additional
 * privacy budget, and no captured row or identifier is ever inserted.</p>
 *
 * <p><b>Legacy profiles</b> (without {@code dpRelease}) fall back to statistics computed
 * directly from {@code sanitizedRealData}; that path offers no formal guarantee and is
 * reported as {@code LEGACY_UNPROTECTED} in the validation report. Inserting raw
 * sanitized rows requires the explicit opt-in {@code loadtest.synthetic.seedRealRows=true}.</p>
 *
 * <p>Generation uses a single seeded {@link java.util.Random}, so two runs with the same
 * seed and the same release are byte-identical.</p>
 *
 * <p>Tables are discovered via {@code INFORMATION_SCHEMA.tables} — the generator
 * is schema-agnostic and works with any PostgreSQL schema without modification.
 * Foreign key constraints are discovered from {@code INFORMATION_SCHEMA} and used
 * to sort tables topologically (parents before children) and to generate FK column
 * values that reference existing rows in the parent table.</p>
 *
 * <pre>
 * loadtest.synthetic.rowsPerTable=500
 * loadtest.synthetic.seed=42
 * loadtest.synthetic.seedRealRows=false   # opt-in, debugging only
 * </pre>
 */
@Component
public class SyntheticDataGenerator {

    private final DatabaseDialect adapter;

    public SyntheticDataGenerator(DatabaseDialect adapter) {
        this.adapter = adapter;
    }

    /** For tests — defaults to PostgresDialect. */
    SyntheticDataGenerator() {
        this(new escuelaing.edu.co.infrastructure.dialect.PostgresDialect());
    }

    private static final Logger LOG = Logger.getLogger(SyntheticDataGenerator.class.getName());

    /** Privacy mode reported when the profile carries a {@link DpRelease}. */
    public static final String MODE_DP_RELEASE = "DP_RELEASE";

    /** Privacy mode reported for legacy profiles that only carry raw sanitized rows. */
    public static final String MODE_LEGACY = "LEGACY_UNPROTECTED";

    /**
     * Pseudonym range for identifier columns without a resolvable parent table.
     * Pseudonyms are random and carry no production information; they may coincide
     * numerically with production identifiers, which is harmless because no
     * production row is ever copied.
     */
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

    @Value("${loadtest.synthetic.seed:42}")
    private long seed;


    /**
     * Comma-separated list of column names to exclude from the string parameter pool.
     * Configure in {@code application.properties} (keep out of version control).
     * Columns not listed here are filtered automatically by cardinality.
     */
    @Value("${loadtest.synthetic.sensitiveColumns:}")
    private String sensitiveColumnsRaw;

    /** Opt-in (debugging only): inserts raw sanitized rows from legacy profiles into the mirror. */
    @Value("${loadtest.synthetic.seedRealRows:false}")
    private boolean seedRealRows = false;

    /** Privacy mode of the last {@link #populate} call. */
    private String lastPrivacyMode = MODE_LEGACY;

    /** DP release used by the last {@link #populate} call (null for legacy profiles). */
    private DpRelease lastDpRelease;

    /** Numeric column statistics used by the last populate: col → [mean, std, lower, upper]. */
    private Map<String, double[]> lastNumericStats = Collections.emptyMap();

    /** Categorical pools used by the last populate (values repeated by released count). */
    private Map<String, List<String>> lastColumnPools = Collections.emptyMap();

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

    /** Foreign-key samplers built from the DP release, keyed {@code child_table.fk_column}. */
    private final Map<String, FkSampler> fkSamplers = new HashMap<>();

    /** Shared popularity ranking of each parent table (same hot parents for every child table). */
    private final Map<String, List<Integer>> parentRanking = new HashMap<>();

    /** Table currently being bulk-loaded (lets FK columns find their sampler). */
    private String currentTable;

    /** Foreign-key relations generated from the released degree shape in the last populate. */
    private List<String> lastFkRelationsApplied = Collections.emptyList();

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


    public long getLastRowsGenerated()  { return lastRowsGenerated; }
    public int  getLastTablesPopulated() { return lastTablesPopulated; }
    public int  getRowsPerTable()        { return rowsPerTable; }

    /** Foreign keys whose values followed the released concentration curve in the last populate. */
    public List<String> getLastFkRelationsApplied() { return lastFkRelationsApplied; }

    /** {@value #MODE_DP_RELEASE} or {@value #MODE_LEGACY} for the last populate. */
    public String getLastPrivacyMode()   { return lastPrivacyMode; }

    /** DP release used by the last populate, or {@code null} for legacy profiles. */
    public DpRelease getLastDpRelease()  { return lastDpRelease; }

    /** Numeric statistics (col → [mean, std, lower, upper]) used by the last populate. */
    public Map<String, double[]> getLastNumericStats() { return lastNumericStats; }

    /** Categorical value pools used by the last populate. */
    public Map<String, List<String>> getLastColumnPools() { return lastColumnPools; }

    /** Whether raw sanitized rows may be inserted (opt-in). */
    public boolean isSeedRealRows()      { return seedRealRows; }

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
        String sql = "SELECT * FROM " + quoteIdent(conn, tableName) + " ORDER BY 1";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= cols; i++) {
                    // Skip timestamp columns — DEFAULT NOW() differs between runs
                    int sqlType = meta.getColumnType(i);
                    if (sqlType == java.sql.Types.TIMESTAMP
                            || sqlType == java.sql.Types.TIMESTAMP_WITH_TIMEZONE) continue;
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
     * <p>Each row is inserted individually with its own savepoint so that a single
     * constraint violation does not discard the entire batch for that table.
     * VARCHAR columns with low cardinality (enum-like CHECK constraints) are generated
     * by sampling from values observed in production via {@code sanitizedRealData},
     * preserving referential integrity without requiring schema-specific configuration.</p>
     *
     * @param conn    open connection to the mirror database
     * @param profile Phase 2 load profile used to calibrate distributions
     */
    public void populate(Connection conn, LoadProfile profile) throws SQLException {
        LOG.info("[SyntheticData] Starting mirror database population...");
        pkCache.clear();
        fkSamplers.clear();
        parentRanking.clear();
        List<String> fkApplied = new ArrayList<>();
        conn.setAutoCommit(false);
        try {
            List<String> tables = discoverUserTables(conn);
            lastTablesPopulated = tables.size();

            Map<String, Map<String, String>> fkConstraints = discoverFkConstraints(conn);
            List<String> sortedTables = topologicalSort(tables, fkConstraints);

            truncateTables(conn, sortedTables);

            boolean hasRelease = profile != null && profile.getDpRelease() != null;
            lastDpRelease   = hasRelease ? profile.getDpRelease() : null;
            lastPrivacyMode = hasRelease ? MODE_DP_RELEASE : MODE_LEGACY;
            if (!hasRelease) {
                LOG.warning("[SyntheticData] Profile has no dpRelease — falling back to legacy statistics "
                        + "computed from sanitizedRealData (no formal privacy guarantee).");
            }
            Map<String, double[]> dpStats = hasRelease
                    ? numericStatsFromRelease(profile.getDpRelease())
                    : buildDpStats(profile);
            Map<String, List<String>> columnStringPool = hasRelease
                    ? poolsFromRelease(profile.getDpRelease())
                    : buildColumnStringPool(profile);
            lastNumericStats = dpStats;
            lastColumnPools  = columnStringPool;

            // Discover CHECK constraints to fill any gaps left by the production string pool.
            // Enum-like columns (e.g. status, tier) whose values never appear in query SELECT
            // results are covered here using the schema's own constraint metadata.
            CheckConstraints checks = discoverCheckConstraints(conn);

            // Production-observed values take precedence; CHECK values fill the gaps.
            checks.enumValues().forEach(columnStringPool::putIfAbsent);
            Map<String, double[]> checkNumericBounds = checks.numericBounds();

            if (seedRealRows && !hasRelease) {
                LOG.warning("[SyntheticData] seedRealRows=true — inserting raw sanitized rows (debugging only).");
                insertRealSanitizedData(conn, profile, sortedTables, dpStats, fkConstraints, columnStringPool, checkNumericBounds);
            }

            for (String table : sortedTables) {
                List<ColumnMeta> cols = describeTable(conn, table);
                if (cols.isEmpty()) continue;
                Map<String, String> fkCols = fkConstraints.getOrDefault(table, Collections.emptyMap());
                if (hasRelease) prepareFkSamplers(conn, table, fkCols, profile.getDpRelease(), fkApplied);
                currentTable = table;
                insertRows(conn, table, cols, rowsPerTable, dpStats, fkCols, columnStringPool, checkNumericBounds);
                currentTable = null;
            }
            lastFkRelationsApplied = Collections.unmodifiableList(fkApplied);

            conn.commit();

            // Update planner statistics after bulk load.
            // Without this, pg_statistic is stale post-COPY: the planner assumes
            // default selectivity, falls back to Seq Scan, and produces
            // unrepresentative Mirror Accuracy measurements. VACUUM ANALYZE also
            // reclaims dead pages from the preceding TRUNCATE.
            try (Statement stAnalyze = conn.createStatement()) {
                conn.setAutoCommit(true); // VACUUM cannot run inside an explicit transaction
                for (String table : sortedTables) {
                    stAnalyze.execute("VACUUM ANALYZE " + quoteIdent(conn, table));
                    LOG.info("[SyntheticData] VACUUM ANALYZE completed: " + table);
                }
            } catch (SQLException e) {
                LOG.warning("[SyntheticData] VACUUM ANALYZE failed: " + e.getMessage()
                        + " — mirror latencies may be non-representative");
            } finally {
                conn.setAutoCommit(false); // restore for subsequent operations
            }

            lastRowsGenerated = countTotalRows(conn, tables);
            LOG.info("[SyntheticData] Mirror database populated: " + tables.size()
                    + " tables, " + lastRowsGenerated + " rows.");
        } catch (SQLException | RuntimeException e) {
            try { conn.rollback(); } catch (SQLException rb) { e.addSuppressed(rb); }
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // Schema discovery

    /**
     * Truncates all user tables before each population run, ensuring the benchmark
     * is deterministic across profiles. Tables are listed comma-separated in a single
     * TRUNCATE so PostgreSQL resolves FK ordering automatically via CASCADE.
     * RESTART IDENTITY resets sequences to 1 so generated IDs are byte-identical
     * across runs with the same seed.
     */
    private void truncateTables(Connection conn, List<String> tables) throws SQLException {
        if (tables.isEmpty()) return;
        List<String> quotedTables = new ArrayList<>();
        for (String t : tables) quotedTables.add(quoteIdent(conn, t));
        String tableList = String.join(", ", quotedTables);
        try (java.sql.Statement st = conn.createStatement()) {
            st.execute("TRUNCATE TABLE " + tableList + " RESTART IDENTITY CASCADE");
        }
        LOG.info("[SyntheticData] Tables truncated: " + tables);
    }

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
     * Discovers simple CHECK constraints in the public schema and extracts the allowed
     * values or numeric bounds they impose on individual columns.
     *
     * <p>Two patterns are recognised from the text returned by
     * {@code pg_get_constraintdef()}:</p>
     * <ul>
     *   <li><b>IN / ANY</b> — {@code CHECK (col IN ('A','B'))} or the internal
     *       {@code = ANY (ARRAY[...])} form produced by PostgreSQL. The quoted string
     *       literals are extracted and stored as the allowed value list for that column.
     *       Used in {@link #setColumnValue} to sample realistic enum values instead of
     *       random alphanumeric strings.</li>
     *   <li><b>BETWEEN / range</b> — {@code CHECK (col BETWEEN 0 AND 5)} or the
     *       expanded {@code col >= 0 AND col <= 5} form. The lower and upper bounds are
     *       stored as a two-element array and used to override the DP-calibrated clamp
     *       in {@link #setColumnValue} for numeric columns.</li>
     * </ul>
     *
     * <p>Constraints that do not match either pattern are silently ignored — the
     * generator falls back to its default behaviour for those columns.</p>
     *
     * @param conn open connection to the mirror database
     * @return {@link CheckConstraints} with enum values and numeric bounds discovered
     */
    CheckConstraints discoverCheckConstraints(Connection conn) throws SQLException {

        Map<String, List<String>> enumValues    = new HashMap<>();
        Map<String, double[]>     checkNumericBounds = new HashMap<>();

        String sql = """
                SELECT pg_get_constraintdef(oid) AS def
                FROM pg_constraint
                WHERE contype = 'c'
                  AND connamespace = 'public'::regnamespace
                """;

        java.util.regex.Pattern inPattern = java.util.regex.Pattern.compile(
                "(?i)'([^']+)'");
        java.util.regex.Pattern rangePattern = java.util.regex.Pattern.compile(
                "(?i)(\\w+)\\s*(?:BETWEEN\\s*(\\d+(?:\\.\\d+)?)\\s*AND\\s*(\\d+(?:\\.\\d+)?)" +
                "|>=\\s*(\\d+(?:\\.\\d+)?).*?<=\\s*(\\d+(?:\\.\\d+)?))");

        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String def = rs.getString("def");
                if (def == null) continue;

                // IN / ANY pattern — extract quoted string literals
                if (def.toUpperCase().contains(" IN ") || def.contains("= ANY")) {
                    java.util.regex.Matcher m = inPattern.matcher(def);
                    List<String> values = new ArrayList<>();
                    while (m.find()) values.add(m.group(1));
                    if (!values.isEmpty()) {
                        // Column name is the first identifier before IN or = ANY
                        java.util.regex.Matcher colM = java.util.regex.Pattern
                                .compile("(?i)\\(?([a-zA-Z_][a-zA-Z0-9_]*)\\)?(?:::\\w+)?\\s*(?:=\\s*ANY|IN\\s*\\()")
                                .matcher(def);
                        if (colM.find()) {
                            enumValues.put(colM.group(1).toLowerCase(), values);
                        }
                    }
                }

                // BETWEEN / >= … <= pattern — extract numeric bounds
                java.util.regex.Matcher rm = rangePattern.matcher(def);
                if (rm.find()) {
                    String col = rm.group(1).toLowerCase();
                    try {
                        double lo = Double.parseDouble(
                                rm.group(2) != null ? rm.group(2) : rm.group(4));
                        double hi = Double.parseDouble(
                                rm.group(3) != null ? rm.group(3) : rm.group(5));
                        checkNumericBounds.put(col, new double[]{lo, hi});
                    } catch (NumberFormatException ignored) {}
                }
            }
        }

        if (!enumValues.isEmpty()) {
            LOG.info("[SyntheticData] CHECK enum columns discovered: " + enumValues.keySet());
        }
        if (!checkNumericBounds.isEmpty()) {
            LOG.info("[SyntheticData] CHECK numeric bounds discovered: " + checkNumericBounds.keySet());
        }
        return new CheckConstraints(enumValues, checkNumericBounds);
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
                                  Map<String, Map<String, String>> fkConstraints,
                                  Map<String, List<String>> columnStringPool,
                                  Map<String, double[]> checkNumericBounds) throws SQLException {
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
                    insertRealRow(conn, target, row, tableSchema.get(target), dpStats, fkCols, columnStringPool, checkNumericBounds);
                    conn.releaseSavepoint(sp);
                    totalInserted++;
                } catch (SQLException e) {
                    // Log only the SQLState (constraint type), never e.getMessage() which
                    // can contain the offending value from the real sanitized row.
                    LOG.warning("[SyntheticData] Real row discarded (SQLState=" + e.getSQLState() + ").");
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
                                Map<String, String> fkColumns,
                                Map<String, List<String>> columnStringPool,
                                Map<String, double[]> checkNumericBounds) throws SQLException {
        List<ColumnMeta> insertable = tableCols.stream()
                .filter(c -> c.defaultValue == null || c.defaultValue.isEmpty())
                .toList();
        if (insertable.isEmpty()) return;

        List<String> quotedColNames = new ArrayList<>();
        for (ColumnMeta c : insertable) quotedColNames.add(quoteIdent(conn, c.name));
        String colNames     = String.join(", ", quotedColNames);
        String placeholders = insertable.stream().map(c -> "?").collect(Collectors.joining(", "));
        String sql = "INSERT INTO " + quoteIdent(conn, table) + " (" + colNames + ") VALUES (" + placeholders
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
                    setColumnValue(ps, i + 1, col, dpStats, conn, fkColumns, columnStringPool, checkNumericBounds);
                }
            }
            ps.executeUpdate();
        }
    }

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    /**
     * Inserts {@code rows} synthetic rows into {@code table} using PostgreSQL COPY.
     *
     * <p>COPY streams CSV data directly to the PostgreSQL server, bypassing the SQL
     * parser and per-row WAL overhead of individual INSERT statements. Throughput is
     * typically 50–100× faster than per-row executeUpdate, making volumes of hundreds
     * of thousands of rows practical for both light and nightly pipelines.</p>
     *
     * <p>Rows are generated in batches of {@value #COPY_BATCH_SIZE} to keep heap
     * usage bounded regardless of {@code rowsPerTable}.</p>
     */
    private static final int COPY_BATCH_SIZE = 100_000;

    private void insertRows(Connection conn, String table,
                             List<ColumnMeta> cols, int rows,
                             Map<String, double[]> dpStats,
                             Map<String, String> fkColumns,
                             Map<String, List<String>> columnStringPool,
                             Map<String, double[]> checkNumericBounds) throws SQLException {
        List<ColumnMeta> insertable = cols.stream()
                .filter(c -> c.defaultValue == null || c.defaultValue.isEmpty()
                        || columnStringPool.containsKey(c.name.toLowerCase())
                        || checkNumericBounds.containsKey(c.name.toLowerCase()))
                .toList();
        if (insertable.isEmpty()) return;

        List<String> quotedColNames = new ArrayList<>();
        for (ColumnMeta c : insertable) quotedColNames.add(quoteIdent(conn, c.name));
        String quotedTable = quoteIdent(conn, table);

        try {
            long total = 0;
            for (int start = 0; start < rows; start += COPY_BATCH_SIZE) {
                int batchRows = Math.min(COPY_BATCH_SIZE, rows - start);
                StringBuilder csv = new StringBuilder(batchRows * insertable.size() * 12);
                for (int i = 0; i < batchRows; i++) {
                    for (int j = 0; j < insertable.size(); j++) {
                        if (j > 0) csv.append(',');
                        csv.append(generateCsvValue(insertable.get(j), dpStats, conn,
                                fkColumns, columnStringPool, checkNumericBounds));
                    }
                    csv.append('\n');
                }
                total += adapter.bulkInsert(conn, quotedTable, quotedColNames, csv.toString());
            }
            LOG.info(String.format("[SyntheticData] Table '%s': %d rows inserted.", table, total));
        } catch (Exception e) {
            throw new SQLException("Bulk insert failed for table '" + table + "': " + e.getMessage(), e);
        }
    }

    /**
     * Generates a single CSV-encoded value for the given column.
     * String values are quoted and internal double-quotes are escaped as {@code ""}.
     */
    private String generateCsvValue(ColumnMeta col,
                                     Map<String, double[]> dpStats,
                                     Connection conn,
                                     Map<String, String> fkColumns,
                                     Map<String, List<String>> columnStringPool,
                                     Map<String, double[]> checkNumericBounds) throws SQLException {
        ColumnCategory category = classifyColumn("", col.name);
        if (category == ColumnCategory.IDENTIFIER) {
            String parentTable = fkColumns != null ? fkColumns.get(col.name) : null;
            if (parentTable != null) {
                FkSampler sampler = currentTable != null ? fkSamplers.get(currentTable + "." + col.name) : null;
                if (sampler != null) return String.valueOf(sampler.next(rng));
                Integer existingId = pickExistingPk(conn, parentTable);
                return String.valueOf(existingId != null ? existingId : generateUiiPseudonym());
            }
            return String.valueOf(generateUiiPseudonym());
        }

        double[] stats = dpStats.get(col.name);
        double mean  = stats != null ? stats[0] : defaultMeanFor(col.dataType);
        double sigma = stats != null ? stats[1] : defaultSigmaFor(col.dataType);
        double[] schemaBounds = checkNumericBounds.get(col.name.toLowerCase());
        double minVal = schemaBounds != null ? schemaBounds[0]
                : (stats != null && stats.length > 2 ? stats[2] : 0.01);
        double maxVal = schemaBounds != null ? schemaBounds[1]
                : (stats != null && stats.length > 3 ? stats[3] : 999_999.99);

        return switch (col.dataType.toLowerCase()) {
            case "integer", "int", "int4", "int8", "bigint", "smallint" ->
                    String.valueOf((int) Math.round(clamp(gaussianNoise(mean, sigma),
                            stats != null ? Math.max(minVal, 0) : 1, stats != null ? maxVal : 10_000)));
            case "numeric", "decimal", "real", "double precision", "float8" ->
                    String.valueOf(Math.round(clamp(gaussianNoise(mean, sigma), minVal, maxVal) * 100) / 100.0);
            case "boolean", "bool" ->
                    rng.nextBoolean() ? "true" : "false";
            case "timestamp", "timestamp without time zone", "timestamp with time zone" -> {
                java.time.Instant ts = java.time.Instant.now()
                        .minusSeconds(rng.nextInt(86_400 * 30));
                yield "\"" + TS_FMT.format(ts) + "\"";
            }
            case "char", "character", "character varying", "varchar", "text" -> {
                List<String> colPool = columnStringPool.getOrDefault(col.name, Collections.emptyList());
                String val = colPool.isEmpty()
                        ? randomString(6, 20)
                        : colPool.get(rng.nextInt(colPool.size()));
                yield "\"" + val.replace("\"", "\"\"") + "\"";
            }
            default -> "\"" + randomString(4, 10) + "\"";
        };
    }

    /**
     * Sets the value for a single column in {@code ps} at position {@code idx}.
     *
     * <p>IDENTIFIER columns receive a pseudonym from the UII range or an existing FK id.
     * FEATURE columns are generated as follows:</p>
     * <ul>
     *   <li>Numeric types: Gaussian noise around the DP-calibrated mean, clamped to the
     *       observed production min/max so values stay within any range CHECK constraints.</li>
     *   <li>String types: sampled from the per-column pool built from {@code sanitizedRealData}
     *       when available (preserves enum-like values such as status codes); falls back to a
     *       random alphanumeric string for columns absent from production samples.</li>
     *   <li>Boolean/timestamp: uniform random.</li>
     * </ul>
     */
    private void setColumnValue(PreparedStatement ps, int idx, ColumnMeta col,
                                 Map<String, double[]> dpStats,
                                 Connection conn, Map<String, String> fkColumns,
                                 Map<String, List<String>> columnStringPool,
                                 Map<String, double[]> checkNumericBounds) throws SQLException {
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
        double[] stats  = dpStats.get(col.name);
        double mean     = stats != null ? stats[0] : defaultMeanFor(col.dataType);
        double sigma    = stats != null ? stats[1] : defaultSigmaFor(col.dataType);
        // CHECK constraint bounds take precedence over production-observed min/max:
        // they are the hard schema limits, while the DP-observed range is a soft guide.
        double[] schemaBounds = checkNumericBounds.get(col.name.toLowerCase());
        double minVal = schemaBounds != null ? schemaBounds[0]
                : (stats != null && stats.length > 2 ? stats[2] : 0.01);
        double maxVal = schemaBounds != null ? schemaBounds[1]
                : (stats != null && stats.length > 3 ? stats[3] : 999_999.99);
        switch (col.dataType.toLowerCase()) {
            case "integer", "int", "int4", "int8", "bigint", "smallint" ->
                    ps.setInt(idx, (int) Math.round(clamp(gaussianNoise(mean, sigma),
                            stats != null ? Math.max(minVal, 0) : 1, stats != null ? maxVal : 10_000)));
            case "numeric", "decimal", "real", "double precision", "float8" ->
                    ps.setBigDecimal(idx, BigDecimal.valueOf(
                            Math.round(clamp(gaussianNoise(mean, sigma), minVal, maxVal) * 100) / 100.0));
            case "boolean", "bool" ->
                    ps.setBoolean(idx, rng.nextBoolean());
            case "timestamp", "timestamp without time zone", "timestamp with time zone" ->
                    ps.setTimestamp(idx, java.sql.Timestamp.from(java.time.Instant.now()
                            .minusSeconds(rng.nextInt(86_400 * 30))));
            case "char", "character", "character varying", "varchar", "text" -> {
                // Sample from production-observed values for this column when available.
                // Enum-like columns (status, tier, category) will thus receive valid values
                // without any schema-specific configuration.
                List<String> colPool = columnStringPool.getOrDefault(col.name, Collections.emptyList());
                ps.setString(idx, colPool.isEmpty()
                        ? randomString(6, 20)
                        : colPool.get(rng.nextInt(colPool.size())));
            }
            default -> ps.setString(idx, randomString(4, 10));
        }
    }

    /**
     * Returns a random primary key value from {@code tableName}, caching up to 10 000
     * IDs per table. A larger cache ensures good FK distribution when child tables
     * have hundreds of thousands of rows. Returns {@code null} if the table is empty.
     */
    private Integer pickExistingPk(Connection conn, String tableName) throws SQLException {
        if (!pkCache.containsKey(tableName)) {
            List<Integer> ids = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id FROM " + quoteIdent(conn, tableName) + " ORDER BY id LIMIT 10000");
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) ids.add(rs.getInt(1));
            }
            Collections.shuffle(ids, rng);
            pkCache.put(tableName, ids);
        }
        List<Integer> ids = pkCache.get(tableName);
        return ids.isEmpty() ? null : ids.get(rng.nextInt(ids.size()));
    }

    /**
     * Builds, for each FK column of {@code table} with a released degree shape, a sampler
     * that assigns child rows to parents following the released concentration curve.
     * Parents are ranked by a seeded permutation shared by all child tables of the same
     * parent (assumption: a parent's popularity is common to its child tables).
     */
    private void prepareFkSamplers(Connection conn, String table, Map<String, String> fkCols,
                                   DpRelease release, List<String> applied) throws SQLException {
        if (release.getForeignKeys() == null || release.getForeignKeys().isEmpty()) return;
        for (Map.Entry<String, String> fk : fkCols.entrySet()) {
            String key = table + "." + fk.getKey();
            DpRelease.ForeignKeyDegree shape = release.getForeignKeys().get(key);
            if (shape == null || shape.getRankFractions() == null || shape.getCumulativeShare() == null) continue;
            List<Integer> ranked = parentRanking.get(fk.getValue());
            if (ranked == null) {
                ranked = new ArrayList<>();
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT id FROM " + quoteIdent(conn, fk.getValue()) + " ORDER BY id");
                     ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) ranked.add(rs.getInt(1));
                }
                Collections.shuffle(ranked, rng);
                parentRanking.put(fk.getValue(), ranked);
            }
            if (ranked.isEmpty()) continue;
            fkSamplers.put(key, FkSampler.of(ranked, shape.getRankFractions(), shape.getCumulativeShare()));
            applied.add(key);
            LOG.info(String.format("[SyntheticData] FK %s -> %s follows released degree shape "
                    + "(top 1%% of parents: %.1f%% of rows).", key, fk.getValue(),
                    100 * escuelaing.edu.co.infrastructure.capture.FkDegreeProfiler.shareAt(shape, 1e-2)));
        }
    }

    /**
     * Samples parent ids with probability proportional to a piecewise-constant weight:
     * parents of segment i (ranks between consecutive released fractions) share the
     * released mass {@code share[i] − share[i−1]} equally.
     */
    static final class FkSampler {
        private final int[]    ids;
        private final double[] cumulative;

        private FkSampler(int[] ids, double[] cumulative) {
            this.ids = ids;
            this.cumulative = cumulative;
        }

        static FkSampler of(List<Integer> rankedIds, List<Double> fractions, List<Double> shares) {
            int n = rankedIds.size();
            int[] ids = new int[n];
            for (int i = 0; i < n; i++) ids[i] = rankedIds.get(i);
            double[] cum = new double[n];
            double prevShare = 0, carry = 0, acc = 0;
            int start = 0;
            for (int s = 0; s < fractions.size(); s++) {
                int end = s == fractions.size() - 1 ? n
                        : (int) Math.min(n, Math.max(start, Math.round(fractions.get(s) * n)));
                double mass = Math.max(0.0, shares.get(s) - prevShare) + carry;
                prevShare = Math.max(prevShare, shares.get(s));
                if (end <= start) { carry = mass; continue; }
                carry = 0;
                double w = mass / (end - start);
                for (int i = start; i < end; i++) { acc += w; cum[i] = acc; }
                start = end;
            }
            if (acc <= 0) {                       // degenerate release → uniform
                for (int i = 0; i < n; i++) cum[i] = i + 1;
            }
            return new FkSampler(ids, cum);
        }

        int next(Random rng) {
            double u = rng.nextDouble() * cumulative[cumulative.length - 1];
            int lo = 0, hi = cumulative.length - 1;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (cumulative[mid] > u) hi = mid; else lo = mid + 1;
            }
            return ids[lo];
        }
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
        if (profile != null && profile.getDpRelease() != null) {
            List<String> flat = new ArrayList<>();
            poolsFromRelease(profile.getDpRelease()).values().forEach(flat::addAll);
            return Collections.unmodifiableList(flat);
        }
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

    /**
     * Builds a per-column pool of string values extracted from {@code sanitizedRealData}.
     *
     * <p>Used in {@link #setColumnValue} so that VARCHAR columns with low-cardinality
     * values (enum-like CHECK constraints such as {@code status} or {@code tier}) are
     * populated with values observed in production rather than random alphanumeric
     * strings. The approach is entirely schema-agnostic: no column or table names are
     * hardcoded — the pool is built dynamically from whatever columns appear in the
     * captured profile.</p>
     *
     * <p>High-cardinality columns (likely free-text or PII, cardinality ratio above
     * {@link #STRING_CARDINALITY_THRESHOLD}) are excluded using the same threshold as
     * {@link #buildStringPool}.</p>
     *
     * @return map of column name → observed string values; empty map if profile is null
     */
    Map<String, List<String>> buildColumnStringPool(LoadProfile profile) {
        if (profile == null || profile.getQueries() == null) return Collections.emptyMap();

        Map<String, List<String>> pool = new HashMap<>();
        for (LoadProfile.QueryStats stats : profile.getQueries().values()) {
            if (stats.getSanitizedRealData() == null) continue;
            for (Map<String, Object> row : stats.getSanitizedRealData()) {
                if (row == null) continue;
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    if (entry.getValue() instanceof String s && !s.isBlank()) {
                        pool.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(s);
                    }
                }
            }
        }

        // Exclude high-cardinality columns (likely free-text or PII).
        pool.entrySet().removeIf(e -> {
            long unique = e.getValue().stream().distinct().count();
            return (double) unique / e.getValue().size() > STRING_CARDINALITY_THRESHOLD;
        });

        LOG.info("[SyntheticData] Column string pool built for: " + pool.keySet());
        return pool;
    }

    private Set<String> parseSensitiveColumns() {
        if (sensitiveColumnsRaw == null || sensitiveColumnsRaw.isBlank()) return Collections.emptySet();
        return java.util.Arrays.stream(sensitiveColumnsRaw.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }

    // Legacy statistics (profiles without dpRelease)

    /**
     * LEGACY: computes per-column statistics directly from {@code sanitizedRealData}.
     *
     * <p>Used only for profiles generated before the DP release existed. No noise is
     * applied and the observed min/max are used as clamp bounds, so this path offers
     * no formal privacy guarantee; it is reported as {@value #MODE_LEGACY}.</p>
     *
     * @return map of column name → [mean, std, observedMin, observedMax]
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

        // LEGACY path (profiles without dpRelease): statistics computed directly from raw
        // sanitized rows. Kept for backward compatibility only — offers no formal guarantee.

        Map<String, double[]> dpStats = new HashMap<>();
        for (Map.Entry<String, List<Double>> entry : columnValues.entrySet()) {
            List<Double> values = entry.getValue().stream().filter(v -> v >= 0).toList();
            if (values.size() < 2) continue;
            double m = mean(values);
            double s = std(values, m);
            double dpMean = m;
            double dpStd  = Math.max(0.1, s);
            double obsMin = values.stream().mapToDouble(Double::doubleValue).min().orElse(0.0);
            double obsMax = values.stream().mapToDouble(Double::doubleValue).max().orElse(m * 3 + 1);
            dpStats.put(entry.getKey(), new double[]{dpMean, dpStd, obsMin, obsMax});
        }

        if (!dpStats.isEmpty()) {
            LOG.warning("[SyntheticData] LEGACY statistics (no DP) built for columns " + dpStats.keySet());
        }
        return dpStats;
    }

    // Utilities

    private long countTotalRows(Connection conn, List<String> tables) {
        long total = 0;
        for (String table : tables) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT COUNT(*) FROM " + quoteIdent(conn, table));
                 ResultSet rs = ps.executeQuery()) {
                if (rs.next()) total += rs.getLong(1);
            } catch (SQLException ignored) {}
        }
        return total;
    }

    /**
     * Samples a synthetic value from N(base, sigma²) using the seeded generator.
     *
     * <p>This is <em>data generation</em> (post-processing of already released
     * statistics), not a privacy mechanism: the privacy noise is applied once, at
     * capture time, by {@link escuelaing.edu.co.infrastructure.capture.DpReleaseBuilder}.</p>
     */
    double gaussianNoise(double base, double sigma) {
        return base + rng.nextGaussian() * sigma;
    }

    // -------------------------------------------------------------------------
    // DP release → generator inputs (post-processing, no extra budget)
    // -------------------------------------------------------------------------

    /** Converts released numeric columns into {@code col → [mean, std, lower, upper]}. */
    static Map<String, double[]> numericStatsFromRelease(DpRelease release) {
        Map<String, double[]> out = new HashMap<>();
        if (release == null || release.getNumeric() == null) return out;
        release.getNumeric().forEach((col, c) ->
                out.put(col, new double[]{c.getMean(), c.getStd(), c.getLower(), c.getUpper()}));
        return out;
    }

    /**
     * Converts released histograms into value pools whose frequencies are proportional
     * to the released counts (scaled so each pool has at most 1 000 entries).
     */
    static Map<String, List<String>> poolsFromRelease(DpRelease release) {
        Map<String, List<String>> out = new HashMap<>();
        if (release == null || release.getCategorical() == null) return out;
        release.getCategorical().forEach((col, counts) -> {
            double total = counts.values().stream().mapToDouble(Double::doubleValue).sum();
            if (total <= 0) return;
            double scale = Math.min(1.0, 1000.0 / total);
            List<String> pool = new ArrayList<>();
            counts.forEach((value, count) -> {
                long reps = Math.max(1, Math.round(count * scale));
                for (long i = 0; i < reps; i++) pool.add(value);
            });
            out.put(col, Collections.unmodifiableList(pool));
        });
        return out;
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

    /**
     * Returns the PostgreSQL-quoted form of {@code identifier} using
     * {@code pg_catalog.quote_ident()}, ensuring that table and column names
     * discovered from INFORMATION_SCHEMA are safe to embed in SQL strings
     * even if they contain special characters or reserved words.
     */
    private String quoteIdent(Connection conn, String identifier) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT pg_catalog.quote_ident(?)")) {
            ps.setString(1, identifier);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : "\"" + identifier + "\"";
            }
        }
    }

    private record ColumnMeta(String name, String dataType, boolean nullable, String defaultValue) {}

    /**
     * Result of {@link #discoverCheckConstraints}: the two disjoint constraint maps
     * extracted from the schema's CHECK expressions.
     *
     * @param enumValues    column name → allowed string values (IN / ANY constraints)
     * @param numericBounds column name → [lo, hi] (BETWEEN / range constraints)
     */
    record CheckConstraints(
            Map<String, List<String>> enumValues,
            Map<String, double[]>     numericBounds) {}

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
