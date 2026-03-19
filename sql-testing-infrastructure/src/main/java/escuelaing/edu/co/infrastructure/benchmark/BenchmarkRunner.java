package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.domain.model.BenchmarkResult;
import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.QueryEntry;
import escuelaing.edu.co.domain.model.TestProfile;
import escuelaing.edu.co.domain.model.TransactionRecord;
import escuelaing.edu.co.infrastructure.analysis.QueryRegistryLoader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Orquesta la ejecución del motor de pruebas de carga (Fase 3).
 *
 * <h3>Ciclo de ejecución</h3>
 * <p>El runner itera sobre las {@link TestProfile.Phase} definidas en el
 * {@link TestProfile} activo. Cada fase controla:</p>
 * <ol>
 *   <li><b>targetTps</b> — throughput objetivo (ops/s) en modo CLOSED_LOOP.</li>
 *   <li><b>throughputFunction</b> — STEP (salto brusco) o LINEAR (rampa).</li>
 *   <li><b>mixturePreset</b> — proporción reads/writes para esa fase.</li>
 *   <li><b>measurement</b> — si las muestras se incluyen en el resultado.</li>
 * </ol>
 *
 * <p>Este diseño sigue el modelo de BenchPress (Van Aken et al., SIGMOD 2015
 * §2.1) donde "a phase is defined as (1) a target transaction rate,
 * (2) a transaction mixture, and (3) a time duration in seconds."
 * La variación T=f(t) por fase sigue el modelo de Dyn-YCSB
 * (Sidhanta et al., IEEE SERVICES 2019).</p>
 *
 * <h3>Métricas recolectadas</h3>
 * <ul>
 *   <li>p50, p95, p99 por query — al finalizar la ventana de medición.</li>
 *   <li>{@code slaComplianceRate} — métrica M de Dyn-YCSB: % de operaciones
 *       dentro del SLA declarado en {@code @Req.maxResponseTimeMs}.</li>
 *   <li>{@code throughputTimeSeries} — TPS cada 10 s (BenchPress §4.2).</li>
 *   <li>{@code latencyTimeSeries} — p50/p95/p99 por ventana de 10 s por query.</li>
 * </ul>
 *
 * <h3>Configuración (application.properties)</h3>
 * <pre>
 * loadtest.benchmark.profileName=nightly
 * loadtest.benchmark.testProfile=normal   # light | normal | peak | sustained | wave
 * loadtest.benchmark.thinkTimeMs=100
 * loadtest.benchmark.metricsWindowSecs=10
 * </pre>
 */
@Component
public class BenchmarkRunner {

    private static final Logger LOG = Logger.getLogger(BenchmarkRunner.class.getName());

    @Value("${loadtest.benchmark.profileName:nightly}")
    private String profileName;

    @Value("${loadtest.benchmark.testProfile:normal}")
    private String testProfileName;

    @Value("${loadtest.benchmark.thinkTimeMs:100}")
    private long thinkTimeMs;

    @Value("${loadtest.benchmark.metricsWindowSecs:10}")
    private int metricsWindowSecs;

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
    // API pública
    // -------------------------------------------------------------------------

    /**
     * Ejecuta el benchmark usando el {@link TestProfile} configurado.
     *
     * @param profile   perfil de carga construido en la Fase 2
     * @param commitSha SHA del commit actual (puede ser null)
     * @return resultado con métricas, series de tiempo y veredicto por consulta
     */
    public BenchmarkResult run(LoadProfile profile, String commitSha) {
        TestProfile testProfile = TestProfile.fromName(testProfileName);
        return run(profile, testProfile, commitSha);
    }

    /**
     * Ejecuta el benchmark con un {@link TestProfile} específico.
     * Útil para lanzar distintos perfiles desde el pipeline nocturno.
     *
     * @param profile      perfil de carga de Fase 2
     * @param testProfile  perfil de ejecución dinámico
     * @param commitSha    SHA del commit actual (puede ser null)
     */
    public BenchmarkResult run(LoadProfile profile, TestProfile testProfile, String commitSha) {
        LOG.info("[BenchmarkRunner] Iniciando benchmark '" + profileName
                + "' con TestProfile '" + testProfile.getName() + "'");

        provisioner.provision();

        try (Connection conn = provisioner.openConnection()) {
            dataGenerator.populate(conn, profile);

            PhaseAccumulator accumulator = new PhaseAccumulator();
            for (TestProfile.Phase phase : testProfile.getPhases()) {
                executePhase(conn, profile, testProfile, phase, accumulator);
            }
            return buildResult(accumulator, profile, testProfile, commitSha);
        } catch (SQLException e) {
            throw new RuntimeException("Error durante el benchmark", e);
        }
    }

    // -------------------------------------------------------------------------
    // Ejecución por fase
    // -------------------------------------------------------------------------

    private void executePhase(Connection conn,
                               LoadProfile profile,
                               TestProfile testProfile,
                               TestProfile.Phase phase,
                               PhaseAccumulator accumulator) throws SQLException {

        LOG.info("[BenchmarkRunner] Fase '" + phase.getName() + "': "
                + "tps=" + phase.getTargetTps()
                + ", dur=" + phase.getDurationSecs() + "s"
                + ", mixture=" + phase.getMixturePreset()
                + ", measurement=" + phase.isMeasurement());

        long phaseEndMs   = System.currentTimeMillis() + (long) phase.getDurationSecs() * 1_000;
        long phaseStartMs = System.currentTimeMillis();
        long prevTps      = accumulator.lastTargetTps;
        int  targetTps    = phase.getTargetTps();

        long windowStartMs             = phaseStartMs;
        List<TransactionRecord> windowSamples = new ArrayList<>();

        while (System.currentTimeMillis() < phaseEndMs) {
            long elapsedInPhaseMs = System.currentTimeMillis() - phaseStartMs;
            long phaseMs          = (long) phase.getDurationSecs() * 1_000;

            int effectiveTps = computeEffectiveTps(
                    phase.getThroughputFunction(), prevTps, targetTps,
                    elapsedInPhaseMs, phaseMs);

            long thinkMs = effectiveTps > 0
                    ? Math.max(1L, 1_000L / effectiveTps)
                    : thinkTimeMs;

            for (Map.Entry<String, LoadProfile.QueryStats> entry : selectQueries(profile, phase).entrySet()) {
                TransactionRecord record = queryExecutor.execute(
                        conn, entry.getKey(), entry.getValue(), null);

                if (phase.isMeasurement()) {
                    accumulator.allSamples.add(record);
                    windowSamples.add(record);
                }
            }

            long now = System.currentTimeMillis();
            if (phase.isMeasurement() && (now - windowStartMs) >= (long) metricsWindowSecs * 1_000) {
                long elapsedFromStart = now - accumulator.measurementStartMs;
                emitSnapshots(windowSamples, elapsedFromStart, phase.getName(), accumulator);
                windowSamples.clear();
                windowStartMs = now;
            }

            sleep(thinkMs);
        }

        if (phase.isMeasurement() && !windowSamples.isEmpty()) {
            long elapsedFromStart = System.currentTimeMillis() - accumulator.measurementStartMs;
            emitSnapshots(windowSamples, elapsedFromStart, phase.getName(), accumulator);
        }

        if (phase.isMeasurement() && accumulator.measurementStartMs == 0) {
            accumulator.measurementStartMs = phaseStartMs;
        }

        accumulator.lastTargetTps = targetTps;
        LOG.info("[BenchmarkRunner] Fase '" + phase.getName() + "' completada.");
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
            String sql    = e.getValue().getCapturedSql();
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
    // Series de tiempo (BenchPress §4.2)
    // -------------------------------------------------------------------------

    private void emitSnapshots(List<TransactionRecord> windowSamples,
                                long elapsedMs,
                                String phaseName,
                                PhaseAccumulator accumulator) {
        if (windowSamples.isEmpty()) return;

        double tps = (double) windowSamples.size() / metricsWindowSecs;
        accumulator.throughputSeries.add(BenchmarkResult.ThroughputSnapshot.builder()
                .elapsedMs(elapsedMs)
                .tps(tps)
                .phaseName(phaseName)
                .build());
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
    // Construcción del resultado
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
            String qid      = entry.getKey();
            List<TransactionRecord> qSamples = entry.getValue();

            List<Long> latencies = new ArrayList<>();
            double totalPlanCost = 0.0;
            for (TransactionRecord r : qSamples) {
                latencies.add(r.getLatencyMs());
                totalPlanCost += QueryExecutor.extractPlanCost(r.getExecutionPlan());
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

            // SLA compliance rate — métrica M de Dyn-YCSB
            // El umbral es maxResponseTimeMs declarado en @Req (Fase 1)
            LoadProfile.QueryStats stats = profile.getQueries().get(qid);
            QueryEntry req = queryRegistry.get(qid);
            long slaThresholdMs = (req != null && req.isHasReq())
                    ? req.getMaxResponseTimeMs()
                    : Long.MAX_VALUE;
            long within       = latencies.stream().filter(l -> l <= slaThresholdMs).count();
            double compliance = n > 0 ? (within * 100.0 / n) : 100.0;

            BenchmarkResult.Verdict verdict = BenchmarkResult.Verdict.PASS;
            String failReason = null;
            if (req != null && req.isHasReq() && p95 > req.getMaxResponseTimeMs()) {
                verdict    = BenchmarkResult.Verdict.FAIL;
                failReason = String.format("p95=%.0fms supera maxResponseTimeMs=%dms de @Req",
                        p95, req.getMaxResponseTimeMs());
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
                    .slaComplianceRate(compliance)
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
                .overallVerdict(anyFail
                        ? BenchmarkResult.Verdict.FAIL
                        : BenchmarkResult.Verdict.PASS)
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
    // Utilidades
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
    // Acumulador de fase
    // -------------------------------------------------------------------------

    private static class PhaseAccumulator {
        final List<TransactionRecord>                          allSamples       = new ArrayList<>();
        final List<BenchmarkResult.ThroughputSnapshot>         throughputSeries = new ArrayList<>();
        final Map<String, List<BenchmarkResult.LatencySnapshot>> latencySeries  = new HashMap<>();
        long   measurementStartMs = 0;
        long   lastTargetTps      = 0;
        double peakTps            = 0.0;
    }
}
