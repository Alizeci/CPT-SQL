package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.domain.model.DpRelease;
import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.validation.CardinalityFidelity;
import escuelaing.edu.co.domain.model.validation.DataQuality;
import escuelaing.edu.co.domain.model.validation.FidelitySummary;
import escuelaing.edu.co.domain.model.validation.LatencyFidelity;
import escuelaing.edu.co.domain.model.validation.MirrorAccuracy;
import escuelaing.edu.co.domain.model.validation.ReproducibilityCheck;
import escuelaing.edu.co.domain.model.validation.ValidationReport;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Savepoint;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Fidelity validation: reports how close the mirror database is to production, in two
 * parts:
 *
 * <ul>
 *   <li><b>Data quality</b> — privacy-budget metadata of the differentially private release
 *       and byte-identical reproducibility of the generated data. Always evaluated.</li>
 *   <li><b>Mirror accuracy</b> — empirical latency and cardinality comparison against the
 *       production statistics in the load profile. Skipped ({@code applicable=false})
 *       when the caller says so; the gate does this for the "normal" test profile.</li>
 * </ul>
 *
 * <p>The overall status is PASS when data quality is {@code REPRODUCIBLE} and mirror
 * accuracy is PASS or not applicable. The gate only reports it and never blocks on it.</p>
 */
@Component
public class FidelityValidator {

    private static final Logger LOG = Logger.getLogger(FidelityValidator.class.getName());

    /**
     * Executions per query in {@link #validateLatencyFidelity} (default 50). Enough runs for
     * a stable nearest-rank p95; with very few runs p95 degenerates into the maximum.
     */
    @Value("${loadtest.fidelity.validationRuns:50}")
    private int validationRuns = 50;

    /**
     * Production p95 (ms) below which a query is reported but excluded from the latency
     * aggregate, since relative error is not meaningful at that scale (default 10).
     */
    @Value("${loadtest.fidelity.latencyFloorMs:10}")
    private long latencyFloorMs = 10;

    @Value("${loadtest.capture.rowsPerTable:0}")
    private long captureRowsPerTable;

    private final MirrorDatabaseProvisioner provisioner;
    private final SyntheticDataGenerator    dataGenerator;
    private final QueryExecutor             queryExecutor;

    public FidelityValidator(MirrorDatabaseProvisioner provisioner,
                             SyntheticDataGenerator dataGenerator,
                             QueryExecutor queryExecutor) {
        this.provisioner   = provisioner;
        this.dataGenerator = dataGenerator;
        this.queryExecutor = queryExecutor;
    }

    /**
     * Provisions and populates the mirror database and evaluates data quality and
     * mirror accuracy.
     *
     * @param applicable {@code false} to skip mirror accuracy (reported as {@code N/A})
     * @return report with status PASS or FAIL; a FAIL is not thrown
     * @throws RuntimeException if the mirror database cannot be opened or populated
     */
    public ValidationReport performValidation(LoadProfile profile, boolean applicable) {
        LOG.info("[FidelityValidator] Starting fidelity validation (mirrorAccuracy.applicable=" + applicable + ")...");
        provisioner.provision();

        try (Connection conn = provisioner.openConnection()) {
            dataGenerator.populate(conn, profile);

            // Mirror accuracy runs first, on the initial populate.
            MirrorAccuracy mirrorAccuracy = validateMirrorAccuracy(conn, profile, applicable);

            // Data quality populates twice more to compare checksums.
            DataQuality dataQuality = validateDataQuality(conn, profile);

            boolean dataOk   = "REPRODUCIBLE".equals(dataQuality.getStatus());
            boolean mirrorOk = !mirrorAccuracy.isApplicable()
                    || "PASS".equals(mirrorAccuracy.getStatus());
            String overallStatus = (dataOk && mirrorOk) ? "PASS" : "FAIL";

            LOG.info("[FidelityValidator] Validation: " + overallStatus);

            return ValidationReport.builder()
                    .validationStatus(overallStatus)
                    .generatedAt(Instant.now().toString())
                    .dataQuality(dataQuality)
                    .mirrorAccuracy(mirrorAccuracy)
                    .build();

        } catch (SQLException e) {
            throw new RuntimeException("Fidelity validation failed", e);
        }
    }

    /**
     * Data quality: verifies byte-identical reproducibility and collects the privacy-budget
     * metadata of the differentially private release (zeros for legacy profiles).
     * Status is {@code REPRODUCIBLE} or {@code FAIL}.
     */
    public DataQuality validateDataQuality(Connection conn,
                                           LoadProfile profile) throws SQLException {
        ReproducibilityCheck reproducibility = validateReproducibility(conn, profile);
        // "REPRODUCIBLE" only certifies byte-identical generation; the privacy mode is
        // reported separately and is never inferred from reproducibility.
        String status = reproducibility.isByteIdentical() ? "REPRODUCIBLE" : "FAIL";
        DpRelease release = profile != null ? profile.getDpRelease() : null;
        boolean dp = release != null;

        return DataQuality.builder()
                .privacyMode(dataGenerator.getLastPrivacyMode())
                .epsilonNumeric(dp ? release.getEpsilonNumeric() : 0.0)
                .epsilonCategorical(dp ? release.getEpsilonCategorical() : 0.0)
                .epsilonForeignKey(dp ? release.getEpsilonForeignKey() : 0.0)
                .epsilonTotal(dp ? release.getEpsilonNumeric() + release.getEpsilonCategorical()
                        + release.getEpsilonForeignKey() : 0.0)
                .delta(dp ? release.getDelta() + release.getDeltaForeignKey() : 0.0)
                .dpForeignKeys(dataGenerator.getLastFkRelationsApplied())
                .dpSourceRows(dp ? release.getSourceRows() : 0L)
                .dpReleasedColumns(dp && release.getNumeric() != null
                        ? new java.util.ArrayList<>(release.getNumeric().keySet()) : java.util.List.of())
                .dpSuppressedColumns(dp && release.getSuppressedColumns() != null
                        ? release.getSuppressedColumns() : java.util.List.of())
                .realRowsSeeded(!dp && dataGenerator.isSeedRealRows())
                .featureMechanism(dp ? release.getMechanism()
                        : "none (legacy profile: statistics computed from raw sanitized rows)")
                .identifierMechanism("synthetic pseudonyms / references to synthetic parent rows (no production identifiers)")
                .totalRowsGenerated(dataGenerator.getLastRowsGenerated())
                .schemaCoverage("100% (" + dataGenerator.getLastTablesPopulated() + " tables)")
                .reproducibility(reproducibility)
                .status(status)
                .build();
    }

    /**
     * Mirror accuracy: empirical latency and cardinality comparison. PASS when at least
     * 90 % of the non-excluded latency checks and 90 % of the cardinality checks pass
     * (an empty set passes). When {@code applicable=false}, returns immediately with
     * {@code status="N/A"}.
     */
    public MirrorAccuracy validateMirrorAccuracy(Connection conn,
                                                  LoadProfile profile,
                                                  boolean applicable) {
        long mirrorRowsPerTable = dataGenerator.getRowsPerTable();

        if (!applicable) {
            return MirrorAccuracy.builder()
                    .applicable(false)
                    .captureRowsPerTable(captureRowsPerTable)
                    .mirrorRowsPerTable(mirrorRowsPerTable)
                    .latencySummary(FidelitySummary.builder().build())
                    .latencyDetails(Map.of())
                    .cardinalitySummary(FidelitySummary.builder().build())
                    .cardinalityDetails(Map.of())
                    .status("N/A")
                    .build();
        }

        // Same parameter model as BenchmarkRunner. Without it, random strings never match
        // the mirror's category values, queries return 0 rows and latencies are
        // unrepresentatively low.
        queryExecutor.setStringPool(dataGenerator.buildStringPool(profile));
        queryExecutor.setParameterModel(dataGenerator.getLastNumericStats(),
                                        dataGenerator.getLastColumnPools());
        queryExecutor.reseed(dataGenerator.getSeed());
        Map<String, LatencyFidelity>     latency     = validateLatencyFidelity(conn, profile);
        Map<String, CardinalityFidelity> cardinality = validateCardinalityFidelity(conn, profile);

        boolean latencyPass = passRate(latency.values().stream()
                .filter(l -> !l.isExcluded())
                .map(LatencyFidelity::isPass).toList()) >= 0.90;
        boolean cardinalityPass = cardinality.isEmpty() || passRate(cardinality.values().stream()
                .map(CardinalityFidelity::isPass).toList()) >= 0.90;

        FidelitySummary latencySummary = FidelitySummary.builder()
                .queriesTested((int) latency.values().stream().filter(l -> !l.isExcluded()).count())
                .queriesPassed((int) latency.values().stream()
                        .filter(l -> !l.isExcluded() && l.isPass()).count())
                .meanErrorPct(latency.values().stream().filter(l -> !l.isExcluded())
                        .mapToDouble(LatencyFidelity::getErrorPct).average().orElse(0.0))
                .build();

        FidelitySummary cardinalitySummary = FidelitySummary.builder()
                .queriesTested(cardinality.size())
                .queriesPassed((int) cardinality.values().stream().filter(CardinalityFidelity::isPass).count())
                .meanErrorPct(cardinality.values().stream()
                        .mapToDouble(CardinalityFidelity::getErrorPct).average().orElse(0.0))
                .build();

        return MirrorAccuracy.builder()
                .applicable(true)
                .captureRowsPerTable(captureRowsPerTable)
                .mirrorRowsPerTable(mirrorRowsPerTable)
                .latencySummary(latencySummary)
                .latencyDetails(latency)
                .cardinalitySummary(cardinalitySummary)
                .cardinalityDetails(cardinality)
                .status(latencyPass && cardinalityPass ? "PASS" : "FAIL")
                .build();
    }

    // Sub-validators

    /**
     * Runs each query with captured SQL {@code validationRuns} times and checks that its
     * p95 on the mirror differs by less than
     * {@link SyntheticDataGenerator#LATENCY_ERROR_THRESHOLD_PCT} (10 %) from the
     * production p95 recorded in the load profile.
     */
    public Map<String, LatencyFidelity> validateLatencyFidelity(Connection conn, LoadProfile profile) {
        Map<String, LatencyFidelity> results = new HashMap<>();
        for (Map.Entry<String, LoadProfile.QueryStats> entry : profile.getQueries().entrySet()) {
            String qid = entry.getKey();
            LoadProfile.QueryStats stats = entry.getValue();
            if (stats.getCapturedSql() == null) continue;

            List<Long> latencies = new ArrayList<>();
            for (int i = 0; i < validationRuns; i++) {
                latencies.add(queryExecutor.execute(conn, qid, stats, stats.getCapturedSql()).getLatencyMs());
            }
            Collections.sort(latencies);
            long p95Syn  = (long) percentile(latencies, 95.0);
            long p95Real = (long) stats.getP95Ms();

            double errorPct = p95Real > 0 ? Math.abs(p95Syn - p95Real) * 100.0 / p95Real : 0.0;
            boolean pass    = errorPct < SyntheticDataGenerator.LATENCY_ERROR_THRESHOLD_PCT;
            // Below the latency floor: reported but excluded from the aggregate.
            boolean excluded = p95Real < latencyFloorMs;
            results.put(qid, LatencyFidelity.builder()
                    .queryId(qid).p95RealMs(p95Real).p95SyntheticMs(p95Syn)
                    .errorPct(errorPct).pass(pass).excluded(excluded)
                    .sampleCountReal(stats.getSampleCount()).build());

            LOG.info(String.format("[Validation] Latency %s: p95_real=%d ms, p95_syn=%d ms, error=%.1f%% → %s",
                    qid, p95Real, p95Syn, errorPct, pass ? "PASS" : "FAIL"));
        }
        return results;
    }

    /**
     * Validates cardinality fidelity for write queries (INSERT/UPDATE/DELETE).
     *
     * <p>Compares rows affected in production ({@code avgRowCount} from the profile)
     * with rows affected on the mirror database; passes when the error is below
     * {@link SyntheticDataGenerator#CARDINALITY_ERROR_THRESHOLD_PCT} (5 %). Queries with
     * {@code avgRowCount == 0} (reads) are skipped. Each write is executed inside a
     * savepoint and rolled back to preserve mirror state; a failed write counts as
     * 0 rows.</p>
     */
    public Map<String, CardinalityFidelity> validateCardinalityFidelity(Connection conn,
                                                                         LoadProfile profile) {
        Map<String, CardinalityFidelity> results = new HashMap<>();
        for (Map.Entry<String, LoadProfile.QueryStats> entry : profile.getQueries().entrySet()) {
            String qid = entry.getKey();
            LoadProfile.QueryStats stats = entry.getValue();
            if (stats.getCapturedSql() == null) continue;
            if (stats.getAvgRowCount() == 0) continue;

            double rowsReal      = stats.getAvgRowCount();
            long   rowsSynthetic = countWriteAffectedRows(conn, stats.getCapturedSql());

            double errorPct = rowsReal > 0
                    ? Math.abs(rowsSynthetic - rowsReal) * 100.0 / rowsReal : 0.0;
            boolean pass    = errorPct < SyntheticDataGenerator.CARDINALITY_ERROR_THRESHOLD_PCT;

            results.put(qid, CardinalityFidelity.builder()
                    .queryId(qid).rowsReal((long) rowsReal).rowsSynthetic(rowsSynthetic)
                    .errorPct(errorPct).pass(pass).build());

            LOG.info(String.format("[Validation] Cardinality %s: real=%.0f, syn=%d, error=%.1f%% → %s",
                    qid, rowsReal, rowsSynthetic, errorPct, pass ? "PASS" : "FAIL"));
        }
        return results;
    }

    /**
     * Verifies that generating data with the same seed produces byte-identical
     * results across two consecutive populates (per-table checksums XOR-combined; tables
     * whose checksum fails are ignored). Leaves the mirror populated.
     */
    public ReproducibilityCheck validateReproducibility(Connection conn,
                                                         LoadProfile profile) throws SQLException {
        long seed = dataGenerator.getSeed();

        dataGenerator.setSeed(seed);
        dataGenerator.populate(conn, profile);
        long checksum1 = computeTablesChecksum(conn);

        dataGenerator.setSeed(seed);
        dataGenerator.populate(conn, profile);
        long checksum2 = computeTablesChecksum(conn);

        boolean identical = checksum1 == checksum2;
        LOG.info(String.format("[Validation] Reproducibility: crc1=%d, crc2=%d → %s",
                checksum1, checksum2, identical ? "PASS" : "FAIL"));

        return ReproducibilityCheck.builder()
                .seed(seed).checksum1(checksum1).checksum2(checksum2)
                .byteIdentical(identical).pass(identical).build();
    }

    // Helpers

    private double passRate(List<Boolean> results) {
        if (results.isEmpty()) return 1.0;
        return (double) results.stream().filter(Boolean::booleanValue).count() / results.size();
    }

    private long computeTablesChecksum(Connection conn) throws SQLException {
        long combined = 0;
        for (String table : dataGenerator.discoverUserTables(conn)) {
            try {
                combined ^= dataGenerator.computeChecksum(conn, table);
            } catch (SQLException ignored) {}
        }
        return combined;
    }

    /**
     * Executes a write statement inside a savepoint and rolls back immediately,
     * returning the number of rows affected without mutating the mirror database.
     */
    private long countWriteAffectedRows(Connection conn, String sql) {
        Savepoint sp = null;
        boolean prevAutoCommit = true;
        try {
            prevAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            sp = conn.setSavepoint();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                // Bind synthetic parameters: executing a statement with unbound
                // placeholders fails and would be counted as 0 rows affected.
                queryExecutor.bindParameters(ps, sql);
                long count = ps.executeUpdate();
                conn.rollback(sp);
                return count;
            } catch (SQLException e) {
                LOG.warning("[Validation] Write cardinality check failed: " + e.getMessage());
                return 0;
            }
        } catch (SQLException e) {
            LOG.warning("[Validation] Connection error in countWriteAffectedRows: " + e.getMessage());
            return 0;
        } finally {
            try { if (sp != null) conn.rollback(sp); } catch (SQLException ignored) {}
            try { conn.setAutoCommit(prevAutoCommit); } catch (SQLException ignored) {}
        }
    }

    private double percentile(List<Long> sorted, double p) {
        if (sorted.isEmpty()) return 0.0;
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }
}
