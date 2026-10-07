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
 * Populates the mirror database with synthetic data generated from the protected statistics
 * (the differentially private release, {@link DpRelease}) in the load profile.
 *
 * <p><b>What reaches the mirror:</b> numeric columns are sampled from N(mean, std) using the
 * released mean and standard deviation, clamped to the declared bounds; categorical columns
 * are sampled from the released histogram; foreign-key columns follow the released
 * foreign-key concentration curve over synthetic parent rows; other identifier columns
 * receive random pseudonyms. Because the generator only post-processes the release, it
 * consumes no additional privacy budget, and no captured row or identifier is inserted.</p>
 *
 * <p><b>Profiles without a release</b> fall back to statistics computed directly from
 * {@code sanitizedRealData}. This debugging-only path offers no formal privacy guarantee and
 * is reported as {@value #MODE_LEGACY}. Inserting the raw sanitized rows additionally
 * requires the opt-in {@code loadtest.synthetic.seedRealRows=true}.</p>
 *
 * <p>All randomness comes from one seeded {@link java.util.Random}, so two runs with the same
 * seed and the same load profile produce the same data, except timestamp columns, which are
 * drawn relative to the current time (and are ignored by {@link #computeChecksum}).</p>
 *
 * <p>Base tables of the {@code public} schema are discovered through
 * {@code INFORMATION_SCHEMA}, so no per-application configuration is needed. Foreign keys are
 * discovered as well and used to populate parents before children and to make FK values
 * reference existing parent rows. Catalog queries, {@code TRUNCATE ... CASCADE},
 * {@code VACUUM ANALYZE} and identifier quoting are PostgreSQL-specific; only the bulk load
 * goes through the {@link DatabaseDialect}.</p>
 *
 * <p>Not thread-safe: {@link #populate} keeps per-run state in instance fields.</p>
 *
 * <pre>
 * loadtest.synthetic.rowsPerTable=500
 * loadtest.synthetic.seed=42
 * loadtest.synthetic.seedRealRows=false   # opt-in, debugging only
 * loadtest.synthetic.sensitiveColumns=    # comma-separated, see buildStringPool
 * </pre>
 */
@Component
public class SyntheticDataGenerator {

    private final DatabaseDialect adapter;

    public SyntheticDataGenerator(DatabaseDialect adapter) {
        this.adapter = adapter;
    }

    /** For tests: uses {@link escuelaing.edu.co.infrastructure.dialect.PostgresDialect}. */
    SyntheticDataGenerator() {
        this(new escuelaing.edu.co.infrastructure.dialect.PostgresDialect());
    }

    private static final Logger LOG = Logger.getLogger(SyntheticDataGenerator.class.getName());

    /** Privacy mode reported when the profile carries a {@link DpRelease}. */
    public static final String MODE_DP_RELEASE = "DP_RELEASE";

    /** Privacy mode reported for profiles without a release (raw sanitized rows only). */
    public static final String MODE_LEGACY = "LEGACY_UNPROTECTED";

    /**
     * Inclusive range of random pseudonyms for identifier columns that have no resolvable
     * parent row. Pseudonyms carry no information from the capture environment; they may
     * coincide numerically with real identifiers, which is harmless because no captured row
     * is copied.
     */
    private static final int UII_MIN = 10_001;
    private static final int UII_MAX = 99_999;

    /**
     * Fidelity validation: maximum relative p95 error (%) between the mirror database and
     * the capture environment for a query to pass (exclusive).
     */
    static final double LATENCY_ERROR_THRESHOLD_PCT = 10.0;

    /**
     * Fidelity validation: maximum relative error (%) in rows affected by write queries for a
     * query to pass (exclusive).
     */
    static final double CARDINALITY_ERROR_THRESHOLD_PCT = 5.0;

    /**
     * Distinct/total ratio above which a captured string column is treated as free text or
     * personal data and excluded from the string pools.
     */
    private static final double STRING_CARDINALITY_THRESHOLD = 0.5;

    @Value("${loadtest.synthetic.rowsPerTable:500}")
    private int rowsPerTable;

    @Value("${loadtest.synthetic.seed:42}")
    private long seed;


    /**
     * Comma-separated column names excluded from the string parameter pool built from
     * {@code sanitizedRealData} (see {@link #buildStringPool}). Columns not listed are still
     * filtered by cardinality. Not used when the profile carries a release.
     */
    @Value("${loadtest.synthetic.sensitiveColumns:}")
    private String sensitiveColumnsRaw;

    /**
     * Opt-in, debugging only: inserts raw sanitized rows into the mirror database when the
     * profile has no release. Ignored when a release is present.
     */
    @Value("${loadtest.synthetic.seedRealRows:false}")
    private boolean seedRealRows = false;

    /** Privacy mode of the last {@link #populate} call. */
    private String lastPrivacyMode = MODE_LEGACY;

    /** Release used by the last {@link #populate} call ({@code null} without a release). */
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
     * Primary key values per table, loaded lazily during {@link #populate} and cleared at the
     * start of each call so identifiers from a previous run are never reused.
     */
    private final Map<String, List<Integer>> pkCache = new HashMap<>();

    /** Foreign-key samplers built from the release, keyed {@code child_table.fk_column}. */
    private final Map<String, FkSampler> fkSamplers = new HashMap<>();

    /** Shared popularity ranking of each parent table (same hot parents for every child table). */
    private final Map<String, List<Integer>> parentRanking = new HashMap<>();

    /** Table currently being bulk-loaded (lets FK columns find their sampler). */
    private String currentTable;

    /** Foreign keys generated from the released concentration curve in the last populate. */
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

    /** Release used by the last populate, or {@code null} if the profile had none. */
    public DpRelease getLastDpRelease()  { return lastDpRelease; }

    /** Numeric statistics (col → [mean, std, lower, upper]) used by the last populate. */
    public Map<String, double[]> getLastNumericStats() { return lastNumericStats; }

    /** Categorical value pools used by the last populate. */
    public Map<String, List<String>> getLastColumnPools() { return lastColumnPools; }

    /** Whether raw sanitized rows may be inserted (opt-in). */
    public boolean isSeedRealRows()      { return seedRealRows; }

    // Column classification: feature vs identifier

    /**
     * Column categories used during generation.
     *
     * <ul>
     *   <li>{@code IDENTIFIER}: any column named {@code id} or ending in {@code _id}. Receives
     *       a reference to an existing parent row when it is a foreign key, otherwise a
     *       pseudonym in the inclusive range [{@code UII_MIN}, {@code UII_MAX}], so real
     *       identifiers never enter the mirror database.</li>
     *   <li>{@code FEATURE}: any other column (numbers, strings, booleans, timestamps).
     *       Generated from the released statistics when available, otherwise from defaults.</li>
     * </ul>
     */
    public enum ColumnCategory { FEATURE, IDENTIFIER }

    /**
     * Classifies a column by name only: {@code id} or a {@code _id} suffix (case-insensitive)
     * is {@code IDENTIFIER}, anything else is {@code FEATURE}. {@code tableName} is unused.
     */
    public ColumnCategory classifyColumn(String tableName, String columnName) {
        String col = columnName.toLowerCase();
        if (col.equals("id") || col.endsWith("_id")) {
            return ColumnCategory.IDENTIFIER;
        }
        return ColumnCategory.FEATURE;
    }

    /**
     * Returns a random integer pseudonym in [{@code UII_MIN}, {@code UII_MAX}] (inclusive),
     * drawn from the seeded generator.
     */
    public int generateUiiPseudonym() {
        return UII_MIN + rng.nextInt(UII_MAX - UII_MIN + 1);
    }

    // Checksum for reproducibility verification

    /**
     * Computes a CRC32 checksum over the contents of {@code tableName}, ordered by the first
     * column. Used to verify that two generations with the same seed produce identical data.
     * Timestamp columns and {@code NULL}s are skipped.
     *
     * @param conn      open connection to the mirror database
     * @param tableName table to checksum
     * @return CRC32 over the string form of every non-timestamp value
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
                    // Timestamps depend on the current time (generated or DEFAULT NOW())
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
     * Replaces the contents of every table in the mirror database with {@code rowsPerTable}
     * synthetic rows generated from {@code profile}.
     *
     * <p>Steps: discover tables and foreign keys, order tables parents-first, truncate them,
     * derive numeric statistics and categorical pools (from the release, or from
     * {@code sanitizedRealData} when there is none), add value lists and bounds from simple
     * CHECK constraints for columns the profile does not cover, then bulk-load each table.
     * With a release, FK columns follow the released foreign-key concentration curve; otherwise
     * they reference a uniformly chosen existing parent row.</p>
     *
     * <p>Truncation and loading run in one transaction that is rolled back on failure. After
     * the commit, {@code VACUUM ANALYZE} refreshes planner statistics; its failure is only
     * logged. The connection is left in auto-commit mode.</p>
     *
     * @param conn    open connection to the mirror database
     * @param profile load profile; may be {@code null}, in which case default distributions
     *                are used
     * @throws SQLException if discovery, truncation or loading fails (after rollback)
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

            // CHECK constraints cover enum-like columns (e.g. status, tier) that the profile
            // does not describe.
            CheckConstraints checks = discoverCheckConstraints(conn);

            // Profile pools take precedence; CHECK values only fill the gaps.
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

            // Refresh planner statistics after the bulk load. Without them the planner uses
            // default selectivity estimates and may choose plans (e.g. Seq Scan) that make
            // mirror latencies unrepresentative.
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
     * Empties {@code tables} with a single {@code TRUNCATE ... RESTART IDENTITY CASCADE}, so
     * every run starts from an empty mirror database. Restarting identities makes generated
     * IDs identical across runs with the same seed.
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

    /** Returns the base tables of the {@code public} schema, sorted by name. */
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
     * <p>Two patterns are recognized in the text returned by {@code pg_get_constraintdef()}:</p>
     * <ul>
     *   <li><b>IN / ANY</b>: {@code CHECK (col IN ('A','B'))} or PostgreSQL's
     *       {@code = ANY (ARRAY[...])} form. The quoted literals become the allowed values of
     *       that column; {@link #populate} uses them as its categorical pool when the profile
     *       has none.</li>
     *   <li><b>BETWEEN / range</b>: {@code CHECK (col BETWEEN 0 AND 5)} or the expanded
     *       {@code col >= 0 AND col <= 5} form, with non-negative literals. The bounds replace
     *       the released (or observed) clamp bounds for that numeric column.</li>
     * </ul>
     *
     * <p>Results are keyed by lower-case column name only, so equally named columns in
     * different tables share them. Other constraints are ignored and the affected columns
     * keep their default generation.</p>
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
                        // Column name: the identifier right before IN or = ANY
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
     * Discovers all foreign key constraints in the {@code public} schema.
     *
     * @return map of child_table → (fk_column → parent_table); parents are assumed to have an
     *         integer {@code id} primary key
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
     * Sorts {@code tables} so that each table appears after the tables it references through
     * foreign keys. Tables in an FK cycle are appended at the end in their original order.
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
     * Debugging only ({@code seedRealRows=true} and no release): inserts each row of
     * {@code sanitizedRealData} into the table whose columns best match its keys. The
     * synthetic rows are loaded afterwards by {@link #populate}.
     *
     * <p>Identifier columns are never copied: FK columns reference an existing parent row
     * (or a pseudonym if the parent is still empty) and other identifiers get a pseudonym.
     * Columns with a default are left to the database. A row that violates a constraint is
     * rolled back to its savepoint and discarded; only the SQLState is logged, never the
     * message, which could contain captured values.</p>
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

    /** Rows generated per bulk-insert call; bounds heap usage regardless of row count. */
    private static final int COPY_BATCH_SIZE = 100_000;

    /**
     * Inserts {@code rows} synthetic rows into {@code table} through
     * {@link DatabaseDialect#bulkInsert} (PostgreSQL {@code COPY}), which is much faster than
     * per-row {@code INSERT}s. Rows are generated as CSV in batches of
     * {@value #COPY_BATCH_SIZE}.
     *
     * @throws SQLException wrapping any bulk-insert failure; a single bad row fails the table
     */
    private void insertRows(Connection conn, String table,
                             List<ColumnMeta> cols, int rows,
                             Map<String, double[]> dpStats,
                             Map<String, String> fkColumns,
                             Map<String, List<String>> columnStringPool,
                             Map<String, double[]> checkNumericBounds) throws SQLException {
        // Columns with a DEFAULT are left to the database unless a categorical pool, CHECK
        // bounds or numeric statistics describe them; otherwise every row would get the
        // default (e.g. 0) and ignore the released statistics.
        List<ColumnMeta> insertable = cols.stream()
                .filter(c -> c.defaultValue == null || c.defaultValue.isEmpty()
                        || columnStringPool.containsKey(c.name.toLowerCase())
                        || checkNumericBounds.containsKey(c.name.toLowerCase())
                        || dpStats.containsKey(c.name)
                        || dpStats.containsKey(c.name.toLowerCase()))
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
     * Generates one CSV-encoded value for {@code col} in the current table.
     *
     * <ul>
     *   <li>Identifiers: FK columns use the table's concentration-curve sampler when one
     *       exists, else a uniformly chosen existing parent id, else a pseudonym; other
     *       identifiers get a pseudonym.</li>
     *   <li>Integers and decimals: N(mean, std) clamped to the CHECK bounds, else the released
     *       bounds; decimals are rounded to two places. Without statistics, type defaults
     *       are used (integers clamped to [1, 10 000]).</li>
     *   <li>Strings: sampled from the column's pool, else a random alphanumeric string.</li>
     *   <li>Booleans: uniform. Timestamps: uniform over the last 30 days.</li>
     * </ul>
     *
     * <p>String and timestamp values are quoted, with inner double quotes escaped as
     * {@code ""}.</p>
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
     * Binds a generated value for {@code col} at position {@code idx} of {@code ps}. Used by
     * the debugging-only real-row path for columns missing from a captured row.
     *
     * <p>Same rules as {@link #generateCsvValue}, except that FK columns always use a
     * uniformly chosen existing parent id (no concentration-curve sampler).</p>
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
        // CHECK bounds are hard schema limits and take precedence over the statistics' bounds.
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
                // The pool keeps enum-like columns (status, tier, ...) within valid values.
                List<String> colPool = columnStringPool.getOrDefault(col.name, Collections.emptyList());
                ps.setString(idx, colPool.isEmpty()
                        ? randomString(6, 20)
                        : colPool.get(rng.nextInt(colPool.size())));
            }
            default -> ps.setString(idx, randomString(4, 10));
        }
    }

    /**
     * Returns a uniformly chosen {@code id} from {@code tableName}, or {@code null} if the
     * table is empty. The first call per table caches the 10 000 lowest ids; later calls
     * reuse that cache until the next {@link #populate}, even if the table grows.
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
     * Builds, for each FK column of {@code table} with a released foreign-key concentration
     * curve, a sampler that assigns child rows to parents following that curve, and records
     * the key in {@code applied}. Parents are ranked by a seeded permutation shared by all
     * child tables of the same parent (assumption: a parent's popularity is common to its
     * child tables). FKs whose parent table is empty are skipped.
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
     * released mass {@code share[i] − share[i−1]} equally. A segment that rounds to no parents
     * passes its mass to the next one; a release with no positive mass yields a uniform sampler.
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
     * Builds the pool of string values that {@link QueryExecutor} uses to bind VARCHAR
     * parameters during load replay.
     *
     * <p>Values are kept with repetition so that uniform sampling from the pool reproduces
     * their frequencies (a value in 70 % of rows is drawn about 70 % of the time). With a
     * release, the pool is the concatenation of all released categorical pools (see
     * {@link #poolsFromRelease}). Without one, values come from {@code sanitizedRealData},
     * excluding columns listed in {@code loadtest.synthetic.sensitiveColumns} and columns whose
     * distinct/total ratio exceeds {@link #STRING_CARDINALITY_THRESHOLD}.</p>
     *
     * @return unmodifiable list of string values; empty if none are available
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
     * Fallback for profiles without a release: builds per-column categorical pools from
     * {@code sanitizedRealData} (values kept with repetition), so enum-like string columns
     * such as {@code status} receive captured values instead of random strings. Columns whose
     * distinct/total ratio exceeds {@link #STRING_CARDINALITY_THRESHOLD} are excluded;
     * {@code sensitiveColumns} is not applied here.
     *
     * @return map of column name → string values; empty if the profile has no queries
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

    // Fallback statistics (profiles without a release)

    /**
     * Fallback for profiles without a release: computes per-column statistics directly from
     * the numeric values in {@code sanitizedRealData}.
     *
     * <p>No noise is added and the observed min/max become the clamp bounds, so this
     * debugging-only path offers no formal privacy guarantee; it is reported as
     * {@value #MODE_LEGACY}. Negative values are ignored, columns with fewer than two values
     * are skipped, and the standard deviation is floored at 0.1.</p>
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
     * <p>This is data generation (post-processing of released statistics), not a privacy
     * mechanism: privacy noise is added once, in the capture environment, by
     * {@link escuelaing.edu.co.infrastructure.capture.DpReleaseBuilder}.</p>
     */
    double gaussianNoise(double base, double sigma) {
        return base + rng.nextGaussian() * sigma;
    }

    // Release → generator inputs (post-processing, no extra privacy budget)

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
     * to the released counts, scaled to about 1 000 entries per pool. Every released value
     * appears at least once; histograms with a non-positive total are skipped.
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
     * Quotes {@code identifier} with {@code pg_catalog.quote_ident()} so discovered table and
     * column names are safe to embed in SQL, even with special characters or reserved words.
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
