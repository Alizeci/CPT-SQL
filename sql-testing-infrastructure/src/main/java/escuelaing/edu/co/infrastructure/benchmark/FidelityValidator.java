package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.validation.CardinalityFidelity;
import escuelaing.edu.co.domain.model.validation.FidelitySummary;
import escuelaing.edu.co.domain.model.validation.LatencyFidelity;
import escuelaing.edu.co.domain.model.validation.ReproducibilityCheck;
import escuelaing.edu.co.domain.model.validation.SyntheticDataInfo;
import escuelaing.edu.co.domain.model.validation.ValidationReport;
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
 * Validates the fidelity of synthetic data generated for the mirror database
 * across three dimensions: latency, cardinality, and reproducibility.
 *
 * <p>Pass criterion: ≥ 90 % of queries must pass each dimension.</p>
 */
@Component
public class FidelityValidator {

    private static final Logger LOG = Logger.getLogger(FidelityValidator.class.getName());

    private static final int VALIDATION_RUNS = 5;

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
     * Provisions the mirror database, populates it, then runs all three validation
     * dimensions and returns the compiled {@link ValidationReport}.
     */
    public ValidationReport performValidation(LoadProfile profile) {
        LOG.info("[FidelityValidator] Starting fidelity validation...");
        provisioner.provision();

        try (Connection conn = provisioner.openConnection()) {
            dataGenerator.populate(conn, profile);

            Map<String, LatencyFidelity>     latency         = validateLatencyFidelity(conn, profile);
            Map<String, CardinalityFidelity> cardinality     = validateCardinalityFidelity(conn, profile);
            ReproducibilityCheck             reproducibility = validateReproducibility(conn, profile);

            boolean latencyPass     = passRate(latency.values().stream()
                    .map(LatencyFidelity::isPass).toList()) >= 0.90;
            boolean cardinalityPass = cardinality.isEmpty() || passRate(cardinality.values().stream()
                    .map(CardinalityFidelity::isPass).toList()) >= 0.90;
            boolean allPass = latencyPass && cardinalityPass && reproducibility.isPass();

            FidelitySummary latencySummary = FidelitySummary.builder()
                    .queriesTested(latency.size())
                    .queriesPassed((int) latency.values().stream().filter(LatencyFidelity::isPass).count())
                    .meanErrorPct(latency.values().stream()
                            .mapToDouble(LatencyFidelity::getErrorPct).average().orElse(0.0))
                    .build();

            FidelitySummary cardinalitySummary = FidelitySummary.builder()
                    .queriesTested(cardinality.size())
                    .queriesPassed((int) cardinality.values().stream().filter(CardinalityFidelity::isPass).count())
                    .meanErrorPct(cardinality.values().stream()
                            .mapToDouble(CardinalityFidelity::getErrorPct).average().orElse(0.0))
                    .build();

            SyntheticDataInfo syntheticInfo = SyntheticDataInfo.builder()
                    .epsilonFt(SyntheticDataGenerator.EPSILON_FT)
                    .epsilonUii(SyntheticDataGenerator.EPSILON_UII)
                    .epsilonTotal(SyntheticDataGenerator.EPSILON_TOTAL)
                    .delta(1e-6)
                    .totalRowsGenerated(dataGenerator.getLastRowsGenerated())
                    .schemaCoverage("100% (" + dataGenerator.getLastTablesPopulated() + " tables)")
                    .build();

            String notes = allPass
                    ? "All dimensions pass threshold. Proceed with benchmark."
                    : String.format("Validation FAIL — latency:%s cardinality:%s reproducibility:%s",
                            latencyPass ? "PASS" : "FAIL",
                            cardinalityPass ? "PASS" : "FAIL",
                            reproducibility.isPass() ? "PASS" : "FAIL");

            ValidationReport report = ValidationReport.builder()
                    .validationStatus(allPass ? ValidationReport.Status.PASS : ValidationReport.Status.FAIL)
                    .generatedAt(Instant.now())
                    .seed(dataGenerator.getSeed())
                    .syntheticDataGeneration(syntheticInfo)
                    .latencySummary(latencySummary)
                    .latencyFidelity(latency)
                    .cardinalitySummary(cardinalitySummary)
                    .cardinalityFidelity(cardinality)
                    .reproducibility(reproducibility)
                    .pass(allPass)
                    .notes(notes)
                    .build();

            LOG.info("[FidelityValidator] Validation: " + report.getValidationStatus());
            return report;

        } catch (SQLException e) {
            throw new RuntimeException("Fidelity validation failed", e);
        }
    }

    /**
     * Validates that p95 on synthetic data differs by less than
     * {@link SyntheticDataGenerator#LATENCY_ERROR_THRESHOLD_PCT} from the
     * production p95 recorded in the load profile.
     */
    public Map<String, LatencyFidelity> validateLatencyFidelity(Connection conn, LoadProfile profile) {
        Map<String, LatencyFidelity> results = new HashMap<>();
        for (Map.Entry<String, LoadProfile.QueryStats> entry : profile.getQueries().entrySet()) {
            String qid = entry.getKey();
            LoadProfile.QueryStats stats = entry.getValue();
            if (stats.getCapturedSql() == null) continue;

            List<Long> latencies = new ArrayList<>();
            for (int i = 0; i < VALIDATION_RUNS; i++) {
                latencies.add(queryExecutor.execute(conn, qid, stats, stats.getCapturedSql()).getLatencyMs());
            }
            Collections.sort(latencies);
            long p95Syn  = (long) percentile(latencies, 95.0);
            long p95Real = (long) stats.getP95Ms();

            double errorPct = p95Real > 0 ? Math.abs(p95Syn - p95Real) * 100.0 / p95Real : 0.0;
            boolean pass    = errorPct < SyntheticDataGenerator.LATENCY_ERROR_THRESHOLD_PCT;

            results.put(qid, LatencyFidelity.builder()
                    .queryId(qid).p95RealMs(p95Real).p95SyntheticMs(p95Syn)
                    .errorPct(errorPct).pass(pass).build());

            LOG.info(String.format("[Validation] Latency %s: p95_real=%d ms, p95_syn=%d ms, error=%.1f%% → %s",
                    qid, p95Real, p95Syn, errorPct, pass ? "PASS" : "FAIL"));
        }
        return results;
    }

    /**
     * Validates cardinality fidelity for write queries (INSERT/UPDATE/DELETE).
     *
     * <p>Compares rows affected in production ({@code avgRowCount} from the profile)
     * with rows affected on the mirror database. SELECT queries ({@code avgRowCount == 0})
     * are skipped. Each write is executed inside a savepoint and rolled back to
     * preserve mirror state.</p>
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
     * results across two consecutive runs (CRC32 over table contents).
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

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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
