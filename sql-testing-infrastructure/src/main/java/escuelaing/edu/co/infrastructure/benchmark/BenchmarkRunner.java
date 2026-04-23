package escuelaing.edu.co.infrastructure.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import escuelaing.edu.co.domain.model.BenchmarkResult;
import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.QueryEntry;
import escuelaing.edu.co.domain.model.TestProfile;
import escuelaing.edu.co.domain.model.TransactionRecord;
import escuelaing.edu.co.domain.model.validation.CardinalityFidelity;
import escuelaing.edu.co.domain.model.validation.FidelitySummary;
import escuelaing.edu.co.domain.model.validation.LatencyFidelity;
import escuelaing.edu.co.domain.model.validation.ReproducibilityCheck;
import escuelaing.edu.co.domain.model.validation.SyntheticDataInfo;
import escuelaing.edu.co.domain.model.validation.ValidationFailedException;
import escuelaing.edu.co.domain.model.validation.ValidationReport;
import escuelaing.edu.co.infrastructure.analysis.QueryRegistryLoader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Savepoint;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Orchestrates Phase 3: provisions the mirror database, populates it with
 * synthetic data, and drives the benchmark execution loop.
 *
 * <p>Iterates over {@link TestProfile} phases in order; only samples from phases
 * marked {@code measurement=true} are included in the result. Throughput is
 * regulated in CLOSED_LOOP mode — the runner sleeps between iterations to match
 * the target TPS. LINEAR phases interpolate throughput from the previous phase's
 * target; STEP phases jump immediately to the new target.</p>
 *
 * <p>Metrics computed per query: p50, p95, p99, mean, min, max latency;
 * {@code slaComplianceRate} (% of operations within {@code @Req.maxResponseTimeMs});
 * average planner cost from EXPLAIN ANALYZE; TPS and latency time-series
 * sampled every {@code metricsWindowSecs} seconds.</p>
 *
 * <pre>
 * loadtest.benchmark.profileName=nightly
 * loadtest.benchmark.testProfile=normal   # light | normal | peak | sustained | wave
 * loadtest.benchmark.thinkTimeMs=100
 * loadtest.benchmark.metricsWindowSecs=10
 * loadtest.sla.complianceThresholdPct=95.0
 * </pre>
 */
@Component
public class BenchmarkRunner {

    private static final Logger LOG = Logger.getLogger(BenchmarkRunner.class.getName());

    /** Number of executions per query during latency fidelity validation. */
    private static final int VALIDATION_RUNS = 5;

    @Value("${loadtest.benchmark.profileName:nightly}")
    private String profileName;

    @Value("${loadtest.benchmark.testProfile:normal}")
    private String testProfileName;

    @Value("${loadtest.benchmark.thinkTimeMs:100}")
    private long thinkTimeMs;

    @Value("${loadtest.benchmark.metricsWindowSecs:10}")
    private int metricsWindowSecs;

    /**
     * Minimum SLA compliance rate for a query verdict to be PASS.
     * If {@code slaComplianceRate} falls below this threshold the query is marked FAIL.
     */
    @Value("${loadtest.sla.complianceThresholdPct:95.0}")
    private double slaComplianceThresholdPct;

    private final MirrorDatabaseProvisioner provisioner;
    private final SyntheticDataGenerator    dataGenerator;
    private final QueryExecutor             queryExecutor;
    private final QueryRegistryLoader       queryRegistry;

    public BenchmarkRunner(MirrorDatabaseProvisioner provisioner,
                           SyntheticDataGenerator dataGenerator,
                           QueryExecutor queryExecutor,
                           QueryRegistryLoader queryRegistry) {
        this.provisioner   = provisioner;
        this.dataGenerator = dataGenerator;
        this.queryExecutor = queryExecutor;
        this.queryRegistry = queryRegistry;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /** Runs the benchmark using the test profile configured in application.properties. */
    public BenchmarkResult run(LoadProfile profile, String commitSha) {
        return run(profile, TestProfile.fromName(testProfileName), commitSha);
    }

    /** Runs the benchmark with an explicit test profile. */
    public BenchmarkResult run(LoadProfile profile, TestProfile testProfile, String commitSha) {
        LOG.info("[BenchmarkRunner] Starting benchmark '" + profileName
                + "' with TestProfile '" + testProfile.getName() + "'");

        provisioner.provision();

        try (Connection conn = provisioner.openConnection()) {
            dataGenerator.populate(conn, profile);
            queryExecutor.setStringPool(dataGenerator.buildStringPool(profile));

            PhaseAccumulator accumulator = new PhaseAccumulator();
            for (TestProfile.Phase phase : testProfile.getPhases()) {
                executePhase(conn, profile, testProfile, phase, accumulator);
            }
            BenchmarkResult result = buildResult(accumulator, profile, testProfile, commitSha);
            persistBenchmarkResult(result);
            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Benchmark execution failed", e);
        }
    }

    /**
     * Runs fidelity validation before the benchmark.
     * Throws {@link ValidationFailedException} if validation does not pass.
     */
    public BenchmarkResult runWithValidation(LoadProfile profile, String commitSha, boolean validate) {
        if (validate) {
            ValidationReport report = performValidation(profile);
            persistValidationReport(report);
            if (!report.isPass()) {
                throw new ValidationFailedException(report);
            }
        }
        return run(profile, commitSha);
    }

    // -------------------------------------------------------------------------
    // Phase execution
    // -------------------------------------------------------------------------

    private void executePhase(Connection conn,
                               LoadProfile profile,
                               TestProfile testProfile,
                               TestProfile.Phase phase,
                               PhaseAccumulator accumulator) throws SQLException {

        LOG.info("[BenchmarkRunner] Phase '" + phase.getName() + "': "
                + "tps=" + phase.getTargetTps()
                + ", dur=" + phase.getDurationSecs() + "s"
                + ", mixture=" + phase.getMixturePreset()
                + ", measurement=" + phase.isMeasurement());

        long phaseEndMs   = System.currentTimeMillis() + (long) phase.getDurationSecs() * 1_000;
        long phaseStartMs = System.currentTimeMillis();
        long prevTps      = accumulator.lastTargetTps;
        int  targetTps    = phase.getTargetTps();

        long windowStartMs = phaseStartMs;
        List<TransactionRecord> windowSamples = new ArrayList<>();

        while (System.currentTimeMillis() < phaseEndMs) {
            long elapsedInPhaseMs = System.currentTimeMillis() - phaseStartMs;
            long phaseMs          = (long) phase.getDurationSecs() * 1_000;

            int effectiveTps = computeEffectiveTps(
                    phase.getThroughputFunction(), prevTps, targetTps, elapsedInPhaseMs, phaseMs);

            long thinkMs = effectiveTps > 0 ? Math.max(1L, 1_000L / effectiveTps) : thinkTimeMs;

            for (Map.Entry<String, LoadProfile.QueryStats> entry : selectQueries(profile, phase).entrySet()) {
                TransactionRecord record = queryExecutor.execute(
                        conn, entry.getKey(), entry.getValue(), null,
                        testProfile.getAccessDistribution(), testProfile.getZipfAlpha());

                if (phase.isMeasurement()) {
                    accumulator.allSamples.add(record);
                    windowSamples.add(record);
                }
            }

            long now = System.currentTimeMillis();
            if (phase.isMeasurement() && (now - windowStartMs) >= (long) metricsWindowSecs * 1_000) {
                emitSnapshots(windowSamples, now - accumulator.measurementStartMs, phase.getName(), accumulator);
                windowSamples.clear();
                windowStartMs = now;
            }

            sleep(thinkMs);
        }

        if (phase.isMeasurement() && !windowSamples.isEmpty()) {
            emitSnapshots(windowSamples,
                    System.currentTimeMillis() - accumulator.measurementStartMs,
                    phase.getName(), accumulator);
        }

        if (phase.isMeasurement() && accumulator.measurementStartMs == 0) {
            accumulator.measurementStartMs = phaseStartMs;
        }

        accumulator.lastTargetTps = targetTps;
        LOG.info("[BenchmarkRunner] Phase '" + phase.getName() + "' completed.");
    }

    private int computeEffectiveTps(TestProfile.ThroughputFunction fn,
                                    long prevTps, int targetTps,
                                    long elapsedMs, long totalMs) {
        if (fn == TestProfile.ThroughputFunction.LINEAR && totalMs > 0) {
            double progress = Math.min(1.0, (double) elapsedMs / totalMs);
            return (int) (prevTps + (targetTps - prevTps) * progress);
        }
        return targetTps;
    }

    private Map<String, LoadProfile.QueryStats> selectQueries(LoadProfile profile,
                                                               TestProfile.Phase phase) {
        if (phase.getMixturePreset() == TestProfile.MixturePreset.DEFAULT) {
            return profile.getQueries();
        }
        Map<String, LoadProfile.QueryStats> filtered = new HashMap<>();
        for (Map.Entry<String, LoadProfile.QueryStats> e : profile.getQueries().entrySet()) {
            String sql     = e.getValue().getCapturedSql();
            boolean isRead = sql == null || sql.trim().toUpperCase().startsWith("SELECT");
            if (phase.getMixturePreset() == TestProfile.MixturePreset.READ_HEAVY && isRead) {
                filtered.put(e.getKey(), e.getValue());
            } else if (phase.getMixturePreset() == TestProfile.MixturePreset.WRITE_HEAVY && !isRead) {
                filtered.put(e.getKey(), e.getValue());
            }
        }
        return filtered.isEmpty() ? profile.getQueries() : filtered;
    }

    // -------------------------------------------------------------------------
    // Time-series snapshots
    // -------------------------------------------------------------------------

    private void emitSnapshots(List<TransactionRecord> windowSamples,
                                long elapsedMs,
                                String phaseName,
                                PhaseAccumulator accumulator) {
        if (windowSamples.isEmpty()) return;

        double tps = (double) windowSamples.size() / metricsWindowSecs;
        accumulator.throughputSeries.add(BenchmarkResult.ThroughputSnapshot.builder()
                .elapsedMs(elapsedMs).tps(tps).phaseName(phaseName).build());
        if (tps > accumulator.peakTps) accumulator.peakTps = tps;

        Map<String, List<TransactionRecord>> byQuery = new HashMap<>();
        for (TransactionRecord r : windowSamples) {
            byQuery.computeIfAbsent(r.getQueryId(), k -> new ArrayList<>()).add(r);
        }
        for (Map.Entry<String, List<TransactionRecord>> e : byQuery.entrySet()) {
            List<Long> lats = new ArrayList<>();
            for (TransactionRecord r : e.getValue()) lats.add(r.getLatencyMs());
            Collections.sort(lats);
            accumulator.latencySeries
                    .computeIfAbsent(e.getKey(), k -> new ArrayList<>())
                    .add(BenchmarkResult.LatencySnapshot.builder()
                            .elapsedMs(elapsedMs)
                            .p50Ms(percentile(lats, 50.0))
                            .p95Ms(percentile(lats, 95.0))
                            .p99Ms(percentile(lats, 99.0))
                            .phaseName(phaseName)
                            .build());
        }
    }

    // -------------------------------------------------------------------------
    // Result aggregation
    // -------------------------------------------------------------------------

    private BenchmarkResult buildResult(PhaseAccumulator accumulator,
                                         LoadProfile profile,
                                         TestProfile testProfile,
                                         String commitSha) {
        List<TransactionRecord> samples = accumulator.allSamples;
        Map<String, List<TransactionRecord>> byQuery = new HashMap<>();
        for (TransactionRecord r : samples) {
            byQuery.computeIfAbsent(r.getQueryId(), k -> new ArrayList<>()).add(r);
        }

        long measurementMs = Math.max(totalMeasurementMs(testProfile), 1L);
        double windowMin   = measurementMs / 60_000.0;

        Map<String, BenchmarkResult.QueryResult> queryResults = new HashMap<>();
        boolean anyFail = false;

        for (Map.Entry<String, List<TransactionRecord>> entry : byQuery.entrySet()) {
            String qid = entry.getKey();
            List<TransactionRecord> qSamples = entry.getValue();

            List<Long> latencies = new ArrayList<>();
            double totalPlanCost = 0.0;
            String lastPlanText = null;
            for (TransactionRecord r : qSamples) {
                latencies.add(r.getLatencyMs());
                totalPlanCost += QueryExecutor.extractPlanCost(r.getExecutionPlan());
                if (r.getExecutionPlan() != null && !r.getExecutionPlan().isBlank()) {
                    lastPlanText = r.getExecutionPlan();
                }
            }
            Collections.sort(latencies);

            long   n      = latencies.size();
            double mean   = latencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
            double median = percentile(latencies, 50.0);
            double p95    = percentile(latencies, 95.0);
            double p99    = percentile(latencies, 99.0);
            long   min    = latencies.isEmpty() ? 0 : latencies.get(0);
            long   max    = latencies.isEmpty() ? 0 : latencies.get(latencies.size() - 1);
            double cpm    = n / windowMin;
            double avgPlan = n > 0 ? totalPlanCost / n : 0.0;

            // slaComplianceRate: % of operations within the declared SLA threshold
            QueryEntry req = queryRegistry.get(qid);
            long slaThresholdMs = (req != null && req.isHasReq())
                    ? req.getMaxResponseTimeMs() : Long.MAX_VALUE;
            long within       = latencies.stream().filter(l -> l <= slaThresholdMs).count();
            double compliance = n > 0 ? (within * 100.0 / n) : 100.0;
            double slaRiskPct = (req != null && req.isHasReq() && req.getMaxResponseTimeMs() > 0)
                    ? (p95 / req.getMaxResponseTimeMs()) * 100.0 : 0.0;

            BenchmarkResult.Verdict verdict = BenchmarkResult.Verdict.PASS;
            String failReason = null;
            if (req != null && req.isHasReq() && p95 > req.getMaxResponseTimeMs()) {
                verdict    = BenchmarkResult.Verdict.FAIL;
                failReason = String.format("p95=%.0fms exceeds maxResponseTimeMs=%dms (@Req)",
                        p95, req.getMaxResponseTimeMs());
                anyFail = true;
            } else if (compliance < slaComplianceThresholdPct) {
                verdict    = BenchmarkResult.Verdict.FAIL;
                failReason = String.format("slaComplianceRate=%.1f%% below threshold %.1f%%",
                        compliance, slaComplianceThresholdPct);
                anyFail = true;
            }

            queryResults.put(qid, BenchmarkResult.QueryResult.builder()
                    .queryId(qid)
                    .sampleCount(n)
                    .meanMs(mean)
                    .medianMs(median)
                    .p95Ms(p95)
                    .p99Ms(p99)
                    .minMs(min)
                    .maxMs(max)
                    .callsPerMinute(cpm)
                    .planCost(avgPlan)
                    .executionPlanText(lastPlanText)
                    .slaComplianceRate(compliance)
                    .slaRiskPct(slaRiskPct)
                    .latencyTimeSeries(accumulator.latencySeries.getOrDefault(qid, List.of()))
                    .verdict(verdict)
                    .failReason(failReason)
                    .build());
        }

        return BenchmarkResult.builder()
                .profileName(profileName)
                .testProfileName(testProfile.getName())
                .executedAt(Instant.now())
                .commitSha(commitSha)
                .totalOperations(samples.size())
                .queries(Collections.unmodifiableMap(queryResults))
                .overallVerdict(anyFail ? BenchmarkResult.Verdict.FAIL : BenchmarkResult.Verdict.PASS)
                .throughputTimeSeries(accumulator.throughputSeries)
                .peakThroughputAchieved(accumulator.peakTps)
                .build();
    }

    private long totalMeasurementMs(TestProfile testProfile) {
        return testProfile.getPhases().stream()
                .filter(TestProfile.Phase::isMeasurement)
                .mapToLong(p -> (long) p.getDurationSecs() * 1_000)
                .sum();
    }

    // -------------------------------------------------------------------------
    // Fidelity validation
    // -------------------------------------------------------------------------

    /**
     * Runs the three fidelity validation dimensions (latency, cardinality,
     * reproducibility) and compiles a {@link ValidationReport}.
     * Pass criterion: ≥ 90 % of queries must pass each dimension.
     */
    public ValidationReport performValidation(LoadProfile profile) {
        LOG.info("[BenchmarkRunner] Starting fidelity validation...");
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

            LOG.info("[BenchmarkRunner] Validation: " + report.getValidationStatus());
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
     * are skipped — row count is unavailable without consuming the ResultSet.
     * Each write is executed inside a savepoint and rolled back to preserve mirror state.</p>
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
    // Persistence
    // -------------------------------------------------------------------------

    /**
     * Persists the result as a versioned JSON file at
     * {@code build/results/benchmark-<profile>-<timestamp>.json}.
     */
    public void persistBenchmarkResult(BenchmarkResult result) {
        try {
            Path dir = Path.of("build", "results");
            Files.createDirectories(dir);
            String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
                    .withZone(ZoneOffset.UTC).format(result.getExecutedAt());
            Path file = dir.resolve("benchmark-" + result.getProfileName() + "-" + timestamp + ".json");
            objectMapper().writeValue(file.toFile(), result);
            LOG.info("[BenchmarkRunner] BenchmarkResult saved to " + file.toAbsolutePath());
        } catch (IOException e) {
            LOG.warning("[BenchmarkRunner] Could not persist BenchmarkResult: " + e.getMessage());
        }
    }

    /** Persists the validation report at {@code build/validation/VALIDATION_REPORT.json}. */
    public void persistValidationReport(ValidationReport report) {
        try {
            Path dir = Path.of("build", "validation");
            Files.createDirectories(dir);
            objectMapper().writeValue(dir.resolve("VALIDATION_REPORT.json").toFile(), report);
            LOG.info("[BenchmarkRunner] ValidationReport saved.");
        } catch (IOException e) {
            LOG.warning("[BenchmarkRunner] Could not persist ValidationReport: " + e.getMessage());
        }
    }

    private ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .enable(SerializationFeature.INDENT_OUTPUT);
    }

    // -------------------------------------------------------------------------
    // Validation helpers
    // -------------------------------------------------------------------------

    private double passRate(List<Boolean> results) {
        if (results.isEmpty()) return 1.0;
        return (double) results.stream().filter(Boolean::booleanValue).count() / results.size();
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

    /**
     * Computes a combined CRC32 checksum over all user tables in the mirror database.
     * Tables are discovered dynamically — no schema-specific names are assumed.
     */
    private long computeTablesChecksum(Connection conn) throws SQLException {
        long combined = 0;
        for (String table : dataGenerator.discoverUserTables(conn)) {
            try {
                combined ^= dataGenerator.computeChecksum(conn, table);
            } catch (SQLException ignored) { /* table not accessible — skip */ }
        }
        return combined;
    }

    // -------------------------------------------------------------------------
    // Utilities
    // -------------------------------------------------------------------------

    private double percentile(List<Long> sorted, double p) {
        if (sorted.isEmpty()) return 0.0;
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    private void sleep(long ms) {
        if (ms <= 0) return;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // -------------------------------------------------------------------------
    // Phase accumulator
    // -------------------------------------------------------------------------

    private static class PhaseAccumulator {
        final List<TransactionRecord>                            allSamples       = new ArrayList<>();
        final List<BenchmarkResult.ThroughputSnapshot>           throughputSeries = new ArrayList<>();
        final Map<String, List<BenchmarkResult.LatencySnapshot>> latencySeries    = new HashMap<>();
        long   measurementStartMs = 0;
        long   lastTargetTps      = 0;
        double peakTps            = 0.0;
    }
}
