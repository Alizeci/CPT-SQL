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

    /**
     * Porcentaje mínimo de operaciones dentro del SLA para que el veredicto sea PASS.
     * Si {@code slaComplianceRate} cae por debajo de este umbral, el veredicto
     * de la consulta se marca como FAIL (§3.4.3).
     * Configurable en {@code application.properties} como
     * {@code loadtest.sla.complianceThresholdPct} (default: 95.0).
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
            BenchmarkResult result = buildResult(accumulator, profile, testProfile, commitSha);
            persistBenchmarkResult(result);
            return result;
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
                        conn, entry.getKey(), entry.getValue(), null,
                        testProfile.getAccessDistribution(), testProfile.getZipfAlpha());

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
            } else if (compliance < slaComplianceThresholdPct) {
                verdict    = BenchmarkResult.Verdict.FAIL;
                failReason = String.format("slaComplianceRate=%.1f%% por debajo del umbral %.1f%%",
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
    // Validación de fidelidad (SynQB — Liu et al., 2024)
    // -------------------------------------------------------------------------

    /**
     * Entry point que ejecuta la validación de fidelidad antes del benchmark.
     *
     * <p>Si {@code validate} es {@code true}, ejecuta las tres dimensiones de
     * validación (latencia, cardinalidad, reproducibilidad) y lanza
     * {@link ValidationFailedException} si alguna falla. Si {@code false},
     * delega directamente a {@link #run(LoadProfile, String)}.</p>
     *
     * @param profile     perfil de carga de Fase 2
     * @param commitSha   SHA del commit actual (puede ser null)
     * @param validate    si {@code true}, realiza la validación previa
     * @return resultado del benchmark
     * @throws ValidationFailedException si la validación no supera los umbrales
     */
    public BenchmarkResult runWithValidation(LoadProfile profile,
                                              String commitSha,
                                              boolean validate) {
        if (validate) {
            ValidationReport report = performValidation(profile);
            persistValidationReport(report);
            if (!report.isPass()) {
                throw new ValidationFailedException(report);
            }
        }
        return run(profile, commitSha);
    }

    /**
     * Orquesta las tres dimensiones de validación de fidelidad y compila el informe.
     *
     * @param profile perfil de carga con estadísticas de producción
     * @return informe consolidado con veredicto global
     */
    public ValidationReport performValidation(LoadProfile profile) {
        LOG.info("[BenchmarkRunner] Iniciando validación de fidelidad (SynQB)...");
        provisioner.provision();

        try (Connection conn = provisioner.openConnection()) {
            dataGenerator.populate(conn, profile);

            Map<String, LatencyFidelity>     latency         = validateLatencyFidelity(conn, profile);
            Map<String, CardinalityFidelity> cardinality     = validateCardinalityFidelity(conn, profile);
            ReproducibilityCheck             reproducibility = validateReproducibility(conn, profile);

            // Criterio ≥ 90 % de queries pasan (SynQB §3.5.1)
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

            int tablesPopulated = dataGenerator.getLastTablesPopulated();
            String schemaCoverage = "100% (" + tablesPopulated + " tables)";

            SyntheticDataInfo syntheticInfo = SyntheticDataInfo.builder()
                    .epsilonFt(SyntheticDataGenerator.EPSILON_FT)
                    .epsilonUii(SyntheticDataGenerator.EPSILON_UII)
                    .epsilonTotal(SyntheticDataGenerator.EPSILON_TOTAL)
                    .delta(1e-6)
                    .totalRowsGenerated(dataGenerator.getLastRowsGenerated())
                    .schemaCoverage(schemaCoverage)
                    .build();

            String notes = allPass
                    ? "All dimensions pass threshold. Proceed with benchmark."
                    : String.format("Validation FAIL — latency:%s cardinality:%s reproducibility:%s",
                            latencyPass ? "PASS" : "FAIL",
                            cardinalityPass ? "PASS" : "FAIL",
                            reproducibility.isPass() ? "PASS" : "FAIL");

            ValidationReport report = ValidationReport.builder()
                    .validationStatus(allPass
                            ? ValidationReport.Status.PASS
                            : ValidationReport.Status.FAIL)
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

            LOG.info("[BenchmarkRunner] Validación: " + report.getValidationStatus());
            return report;

        } catch (SQLException e) {
            throw new RuntimeException("Error durante la validación de fidelidad", e);
        }
    }

    /**
     * Valida que el p95 medido sobre datos sintéticos difiere en menos del 10 %
     * del p95 real registrado en el {@link LoadProfile} (producción).
     *
     * <p>Cada query se ejecuta {@code VALIDATION_RUNS} veces sobre la BD espejo.
     * El p95 sintético se compara con {@link LoadProfile.QueryStats#getP95Ms()}.</p>
     */
    private static final int VALIDATION_RUNS = 5;

    public Map<String, LatencyFidelity> validateLatencyFidelity(Connection conn,
                                                                  LoadProfile profile) {
        Map<String, LatencyFidelity> results = new HashMap<>();
        for (Map.Entry<String, LoadProfile.QueryStats> entry : profile.getQueries().entrySet()) {
            String qid   = entry.getKey();
            LoadProfile.QueryStats stats = entry.getValue();
            if (stats.getCapturedSql() == null) continue;

            List<Long> latencies = new ArrayList<>();
            for (int i = 0; i < VALIDATION_RUNS; i++) {
                TransactionRecord rec = queryExecutor.execute(conn, qid, stats, stats.getCapturedSql());
                latencies.add(rec.getLatencyMs());
            }
            Collections.sort(latencies);
            long p95Syn  = (long) percentile(latencies, 95.0);
            long p95Real = (long) stats.getP95Ms();

            double errorPct = p95Real > 0
                    ? Math.abs(p95Syn - p95Real) * 100.0 / p95Real
                    : 0.0;
            boolean pass = errorPct < SyntheticDataGenerator.LATENCY_ERROR_THRESHOLD_PCT;

            results.put(qid, LatencyFidelity.builder()
                    .queryId(qid)
                    .p95RealMs(p95Real)
                    .p95SyntheticMs(p95Syn)
                    .errorPct(errorPct)
                    .pass(pass)
                    .build());

            LOG.info(String.format("[Validation] Latencia %s: p95_real=%d ms, p95_syn=%d ms, error=%.1f %% → %s",
                    qid, p95Real, p95Syn, errorPct, pass ? "PASS" : "FAIL"));
        }
        return results;
    }

    /**
     * Valida la fidelidad de cardinalidad para queries de escritura (INSERT/UPDATE/DELETE).
     *
     * <p>Compara el promedio de filas afectadas en producción ({@code avgRowCount} del
     * {@link LoadProfile}) con las filas afectadas al ejecutar la misma query sobre la
     * BD espejo. Queries SELECT ({@code avgRowCount == 0}) se omiten — no se puede
     * determinar el conteo de filas sin consumir el ResultSet (limitación documentada
     * en §5.2).</p>
     *
     * <p>La query se ejecuta dentro de un savepoint y se hace rollback para no alterar
     * el estado de la BD espejo antes de la ejecución del benchmark.</p>
     */
    public Map<String, CardinalityFidelity> validateCardinalityFidelity(Connection conn,
                                                                          LoadProfile profile) {
        Map<String, CardinalityFidelity> results = new HashMap<>();
        for (Map.Entry<String, LoadProfile.QueryStats> entry : profile.getQueries().entrySet()) {
            String qid = entry.getKey();
            LoadProfile.QueryStats stats = entry.getValue();
            if (stats.getCapturedSql() == null) continue;
            if (stats.getAvgRowCount() == 0) continue; // SELECT o sin datos — N/A

            double rowsReal      = stats.getAvgRowCount();
            long   rowsSynthetic = countWriteAffectedRows(conn, stats.getCapturedSql());

            double errorPct = rowsReal > 0
                    ? Math.abs(rowsSynthetic - rowsReal) * 100.0 / rowsReal
                    : 0.0;
            boolean pass = errorPct < SyntheticDataGenerator.CARDINALITY_ERROR_THRESHOLD_PCT;

            results.put(qid, CardinalityFidelity.builder()
                    .queryId(qid)
                    .rowsReal((long) rowsReal)
                    .rowsSynthetic(rowsSynthetic)
                    .errorPct(errorPct)
                    .pass(pass)
                    .build());

            LOG.info(String.format("[Validation] Cardinalidad %s: real=%.0f, syn=%d, error=%.1f %% → %s",
                    qid, rowsReal, rowsSynthetic, errorPct, pass ? "PASS" : "FAIL"));
        }
        return results;
    }

    /**
     * Verifica que generar datos con el mismo seed produce resultados byte-identical
     * en dos ejecuciones consecutivas (CRC32 sobre el contenido de las tablas).
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
        LOG.info(String.format("[Validation] Reproducibilidad: crc1=%d, crc2=%d → %s",
                checksum1, checksum2, identical ? "PASS" : "FAIL"));

        return ReproducibilityCheck.builder()
                .seed(seed)
                .checksum1(checksum1)
                .checksum2(checksum2)
                .byteIdentical(identical)
                .pass(identical)
                .build();
    }

    /**
     * Persiste el {@link BenchmarkResult} como JSON versionado en
     * {@code build/results/benchmark-<profile>-<timestamp>.json}.
     *
     * <p>El nombre del artefacto incluye el identificador del perfil de carga y la
     * marca temporal (UTC) de la ejecución, garantizando trazabilidad directa entre
     * commit y métricas (§3.5 Fase 4).</p>
     */
    public void persistBenchmarkResult(BenchmarkResult result) {
        try {
            Path dir = Path.of("build", "results");
            Files.createDirectories(dir);
            String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
                    .withZone(ZoneOffset.UTC)
                    .format(result.getExecutedAt());
            String filename = "benchmark-" + result.getProfileName() + "-" + timestamp + ".json";
            Path file = dir.resolve(filename);
            ObjectMapper mapper = new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                    .enable(SerializationFeature.INDENT_OUTPUT);
            mapper.writeValue(file.toFile(), result);
            LOG.info("[BenchmarkRunner] BenchmarkResult persistido en " + file.toAbsolutePath());
        } catch (IOException e) {
            LOG.warning("[BenchmarkRunner] No se pudo persistir BenchmarkResult: " + e.getMessage());
        }
    }

    /**
     * Persiste el {@link ValidationReport} como JSON en
     * {@code build/validation/VALIDATION_REPORT.json}.
     */
    public void persistValidationReport(ValidationReport report) {
        try {
            Path dir = Path.of("build", "validation");
            Files.createDirectories(dir);
            Path file = dir.resolve("VALIDATION_REPORT.json");
            ObjectMapper mapper = new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                    .enable(SerializationFeature.INDENT_OUTPUT);
            mapper.writeValue(file.toFile(), report);
            LOG.info("[BenchmarkRunner] ValidationReport persistido en " + file.toAbsolutePath());
        } catch (IOException e) {
            LOG.warning("[BenchmarkRunner] No se pudo persistir ValidationReport: " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Helpers de validación
    // -------------------------------------------------------------------------

    /** Devuelve la fracción de valores {@code true} en la lista. */
    private double passRate(List<Boolean> results) {
        if (results.isEmpty()) return 1.0;
        long passed = results.stream().filter(Boolean::booleanValue).count();
        return (double) passed / results.size();
    }

    /**
     * Ejecuta una query de escritura (INSERT/UPDATE/DELETE) sobre la BD espejo
     * dentro de un savepoint y hace rollback para no alterar el estado.
     *
     * @return filas afectadas, o {@code 0} si la ejecución falla
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
                LOG.warning("[Validation] Fallo al ejecutar write para cardinalidad: " + e.getMessage());
                return 0;
            }
        } catch (SQLException e) {
            LOG.warning("[Validation] Error de conexión en countWriteAffectedRows: " + e.getMessage());
            return 0;
        } finally {
            try { if (sp != null) conn.rollback(sp); } catch (SQLException ignored) {}
            try { conn.setAutoCommit(prevAutoCommit); } catch (SQLException ignored) {}
        }
    }

    private long computeTablesChecksum(Connection conn) throws SQLException {
        long combined = 0;
        for (String table : List.of("products", "customers", "orders", "order_items", "inventory_log")) {
            try {
                combined ^= dataGenerator.computeChecksum(conn, table);
            } catch (SQLException ignored) {
                // tabla puede no existir en schemas distintos al e-commerce
            }
        }
        return combined;
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
