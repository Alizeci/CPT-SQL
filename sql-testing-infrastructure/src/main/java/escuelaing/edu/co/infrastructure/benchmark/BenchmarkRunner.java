package escuelaing.edu.co.infrastructure.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import escuelaing.edu.co.domain.model.BenchmarkResult;
import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.QueryEntry;
import escuelaing.edu.co.domain.model.TestProfile;
import escuelaing.edu.co.domain.model.TransactionRecord;
import escuelaing.edu.co.domain.model.validation.ValidationFailedException;
import escuelaing.edu.co.domain.model.validation.ValidationReport;
import escuelaing.edu.co.infrastructure.analysis.QueryRegistryLoader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
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
 * Runs the benchmark: provisions the mirror database, populates it with synthetic data
 * generated from the load profile, and replays the load on a single connection.
 *
 * <p>Iterates over {@link TestProfile} phases in order; only samples from phases
 * marked {@code measurement=true} are included in the result. Each loop iteration
 * executes every query selected by the phase's mixture once, then sleeps
 * {@code 1000 / effectiveTps} ms ({@code thinkTimeMs} when the target is 0). Execution
 * time is not subtracted, so the achieved rate is at most the target. LINEAR phases
 * interpolate the target from the previous phase's; other functions use the phase
 * target directly.</p>
 *
 * <p>Metrics computed per query: p50, p95, p99 (nearest rank), mean, min, max latency;
 * {@code slaComplianceRate} (% of operations within {@code @Req.maxResponseTimeMs});
 * {@code slaRiskPct} (p95 as % of the SLA); average plan cost from EXPLAIN ANALYZE;
 * TPS and latency time series sampled every {@code metricsWindowSecs} seconds.</p>
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

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(SerializationFeature.INDENT_OUTPUT);

    @Value("${loadtest.benchmark.profileName:nightly}")
    private String profileName;

    @Value("${loadtest.benchmark.testProfile:normal}")
    private String testProfileName;

    @Value("${loadtest.benchmark.thinkTimeMs:100}")
    private long thinkTimeMs;

    @Value("${loadtest.benchmark.metricsWindowSecs:10}")
    private int metricsWindowSecs;

    /**
     * Minimum SLA compliance rate (%) for a query verdict to be PASS. A query is FAIL
     * when its p95 exceeds the SLA or its {@code slaComplianceRate} is below this value.
     */
    @Value("${loadtest.sla.complianceThresholdPct:95.0}")
    private double slaComplianceThresholdPct;

    private final MirrorDatabaseProvisioner provisioner;
    private final SyntheticDataGenerator    dataGenerator;
    private final QueryExecutor             queryExecutor;
    private final QueryRegistryLoader       queryRegistry;
    private final FidelityValidator         fidelityValidator;

    public BenchmarkRunner(MirrorDatabaseProvisioner provisioner,
                           SyntheticDataGenerator dataGenerator,
                           QueryExecutor queryExecutor,
                           QueryRegistryLoader queryRegistry,
                           FidelityValidator fidelityValidator) {
        this.provisioner      = provisioner;
        this.dataGenerator    = dataGenerator;
        this.queryExecutor    = queryExecutor;
        this.queryRegistry    = queryRegistry;
        this.fidelityValidator = fidelityValidator;
    }

    // Public API

    /** Runs the benchmark using the test profile configured in {@code loadtest.benchmark.testProfile}. */
    public BenchmarkResult run(LoadProfile profile, String commitSha) {
        return run(profile, TestProfile.fromName(testProfileName), commitSha);
    }

    /**
     * Runs the benchmark with an explicit test profile and persists the result under
     * {@code build/results}.
     *
     * @throws RuntimeException if the mirror database cannot be opened or populated
     */
    public BenchmarkResult run(LoadProfile profile, TestProfile testProfile, String commitSha) {
        LOG.info("[BenchmarkRunner] Starting benchmark '" + profileName
                + "' with TestProfile '" + testProfile.getName() + "'");

        provisioner.provision();

        try (Connection conn = provisioner.openConnection()) {
            dataGenerator.populate(conn, profile);
            queryExecutor.setStringPool(dataGenerator.buildStringPool(profile));
            queryExecutor.setParameterModel(dataGenerator.getLastNumericStats(),
                                            dataGenerator.getLastColumnPools());
            queryExecutor.reseed(dataGenerator.getSeed());

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
     * Optionally runs the fidelity validation (mirror accuracy included) before the
     * benchmark and persists its report. Unlike the gate entry point, this variant is
     * strict: it throws {@link ValidationFailedException} if validation does not pass.
     */
    public BenchmarkResult runWithValidation(LoadProfile profile, String commitSha, boolean validate) {
        if (validate) {
            ValidationReport report = fidelityValidator.performValidation(profile, true);
            persistValidationReport(report);
            if (!report.isPass()) {
                throw new ValidationFailedException(report);
            }
        }
        return run(profile, commitSha);
    }

    // Phase execution

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

        // The measurement start must be set before the loop so that the
        // latency time series records time elapsed since the start of measurement.
        if (phase.isMeasurement() && accumulator.measurementStartMs == 0) {
            accumulator.measurementStartMs = phaseStartMs;
        }

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

    /**
     * Queries run by {@code phase}: all for DEFAULT, reads (SELECT or unknown SQL) for
     * READ_HEAVY, writes for WRITE_HEAVY. Falls back to all queries if the filter is empty.
     */
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

    // Time-series snapshots

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

    // Result aggregation

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
                totalPlanCost += queryExecutor.extractPlanCost(r.getExecutionPlan());
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
                .rowsPerTable(dataGenerator.getRowsPerTable())
                .loadProfileFingerprint(fingerprint(profile))
                .build();
    }

    /**
     * Fingerprint of the inputs that determine the mirror database: the differentially
     * private release, or the raw {@code sanitizedRealData} rows for legacy profiles.
     * Captured SQL and latency statistics are deliberately excluded, so a PR that only
     * changes SQL keeps the same fingerprint.
     *
     * @return first 8 bytes of a SHA-256 digest as 16 hex characters, or {@code null}
     *         if {@code profile} is null or hashing fails
     */
    public static String fingerprint(LoadProfile profile) {
        if (profile == null) return null;
        try {
            com.fasterxml.jackson.databind.ObjectMapper m = new com.fasterxml.jackson.databind.ObjectMapper()
                    .configure(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
                    .configure(com.fasterxml.jackson.databind.MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
            Object basis;
            if (profile.getDpRelease() != null) {
                basis = profile.getDpRelease();
            } else {
                java.util.Map<String, Object> legacy = new java.util.TreeMap<>();
                if (profile.getQueries() != null) {
                    profile.getQueries().forEach((qid, q) -> legacy.put(qid, q.getSanitizedRealData()));
                }
                basis = legacy;
            }
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(m.writeValueAsBytes(basis));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 8; i++) sb.append(String.format("%02x", digest[i]));
            return sb.toString();
        } catch (Exception e) {
            LOG.warning("[BenchmarkRunner] Could not fingerprint load profile: " + e.getMessage());
            return null;
        }
    }

    private long totalMeasurementMs(TestProfile testProfile) {
        return testProfile.getPhases().stream()
                .filter(TestProfile.Phase::isMeasurement)
                .mapToLong(p -> (long) p.getDurationSecs() * 1_000)
                .sum();
    }

    // Persistence

    /**
     * Writes the result to {@code build/results/benchmark-<profile>-<timestamp>.json}
     * (UTC timestamp). I/O errors are logged, not thrown.
     */
    public void persistBenchmarkResult(BenchmarkResult result) {
        try {
            Path dir = Path.of("build", "results");
            Files.createDirectories(dir);
            String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
                    .withZone(ZoneOffset.UTC).format(result.getExecutedAt());
            Path file = dir.resolve("benchmark-" + result.getProfileName() + "-" + timestamp + ".json");
            MAPPER.writeValue(file.toFile(), result);
            LOG.info("[BenchmarkRunner] BenchmarkResult saved to " + file.toAbsolutePath());
        } catch (IOException e) {
            LOG.warning("[BenchmarkRunner] Could not persist BenchmarkResult: " + e.getMessage());
        }
    }

    /**
     * Writes the report to {@code build/validation/VALIDATION_REPORT.json}, overwriting the
     * previous one. I/O errors are logged, not thrown.
     */
    public void persistValidationReport(ValidationReport report) {
        try {
            Path dir = Path.of("build", "validation");
            Files.createDirectories(dir);
            MAPPER.writeValue(dir.resolve("VALIDATION_REPORT.json").toFile(), report);
            LOG.info("[BenchmarkRunner] ValidationReport saved.");
        } catch (IOException e) {
            LOG.warning("[BenchmarkRunner] Could not persist ValidationReport: " + e.getMessage());
        }
    }

    // Utilities

    /** Nearest-rank percentile of an ascending list; 0 when empty. */
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

    /** Mutable state carried across the phases of one run. */
    private static class PhaseAccumulator {
        final List<TransactionRecord>                            allSamples       = new ArrayList<>();
        final List<BenchmarkResult.ThroughputSnapshot>           throughputSeries = new ArrayList<>();
        final Map<String, List<BenchmarkResult.LatencySnapshot>> latencySeries    = new HashMap<>();
        long   measurementStartMs = 0;
        long   lastTargetTps      = 0;
        double peakTps            = 0.0;
    }
}
