package escuelaing.edu.co.infrastructure;

import escuelaing.edu.co.domain.model.BenchmarkResult;
import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.TransactionRecord;
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
 * <pre>
 * 1. Ramp-up  (30 s por defecto): aumenta la tasa de llamadas gradualmente
 *    desde 0 hasta la frecuencia objetivo del perfil de carga.
 * 2. Ventana de medición: ejecuta consultas a la tasa objetivo y registra
 *    latencias.  Solo las muestras de esta fase entran en el resultado.
 * 3. Think time: pausa entre transacciones para simular el comportamiento
 *    de un usuario real y evitar sobrecarga artificial.
 * </pre>
 *
 * <h3>Configuración (application.properties)</h3>
 * <pre>
 * loadtest.benchmark.rampUpSecs=30
 * loadtest.benchmark.measurementSecs=120
 * loadtest.benchmark.thinkTimeMs=100
 * loadtest.benchmark.profileName=nightly
 * </pre>
 */
@Component
public class BenchmarkRunner {

    private static final Logger LOG = Logger.getLogger(BenchmarkRunner.class.getName());

    @Value("${loadtest.benchmark.rampUpSecs:30}")
    private int rampUpSecs;

    @Value("${loadtest.benchmark.measurementSecs:120}")
    private int measurementSecs;

    @Value("${loadtest.benchmark.thinkTimeMs:100}")
    private long thinkTimeMs;

    @Value("${loadtest.benchmark.profileName:nightly}")
    private String profileName;

    private final MirrorDatabaseProvisioner provisioner;
    private final SyntheticDataGenerator dataGenerator;
    private final QueryExecutor queryExecutor;

    public BenchmarkRunner(MirrorDatabaseProvisioner provisioner,
                           SyntheticDataGenerator dataGenerator,
                           QueryExecutor queryExecutor) {
        this.provisioner    = provisioner;
        this.dataGenerator  = dataGenerator;
        this.queryExecutor  = queryExecutor;
    }

    // -------------------------------------------------------------------------
    // API pública
    // -------------------------------------------------------------------------

    /**
     * Ejecuta el benchmark completo para el {@code profile} dado.
     *
     * <p>El flujo es: provision → populate → ramp-up → medición → resultado.</p>
     *
     * @param profile perfil de carga construido en la Fase 2
     * @param commitSha SHA del commit actual (puede ser null)
     * @return resultado con métricas y veredicto por consulta
     */
    public BenchmarkResult run(LoadProfile profile, String commitSha) {
        LOG.info("[BenchmarkRunner] Iniciando benchmark '" + profileName + "'");

        provisioner.provision();

        try (Connection conn = provisioner.openConnection()) {
            dataGenerator.populate(conn, profile);
            runRampUp(conn, profile);
            List<TransactionRecord> samples = runMeasurement(conn, profile);
            return buildResult(samples, profile, commitSha);
        } catch (SQLException e) {
            throw new RuntimeException("Error durante el benchmark", e);
        }
    }

    // -------------------------------------------------------------------------
    // Ramp-up
    // -------------------------------------------------------------------------

    /**
     * Fase de ramp-up: ejecuta consultas a tasa creciente durante
     * {@code rampUpSecs} segundos. Las muestras de esta fase se descartan.
     */
    private void runRampUp(Connection conn, LoadProfile profile) throws SQLException {
        LOG.info("[BenchmarkRunner] Ramp-up: " + rampUpSecs + " s");
        long endMs = System.currentTimeMillis() + (long) rampUpSecs * 1_000;
        int iteration = 0;

        while (System.currentTimeMillis() < endMs) {
            iteration++;
            double progress = (double) (endMs - System.currentTimeMillis())
                    / ((long) rampUpSecs * 1_000);
            // Tasa inicial = 10% del objetivo, crece linealmente
            long targetThink = (long) (thinkTimeMs / Math.max(0.1, 1.0 - progress * 0.9));

            for (String queryId : profile.getQueries().keySet()) {
                queryExecutor.execute(conn, queryId, profile.getQueries().get(queryId), null);
            }
            sleep(targetThink);
        }
        LOG.info("[BenchmarkRunner] Ramp-up completado (" + iteration + " iteraciones).");
    }

    // -------------------------------------------------------------------------
    // Ventana de medición
    // -------------------------------------------------------------------------

    /**
     * Ventana de medición: ejecuta consultas a la tasa objetivo y registra
     * todas las muestras.
     *
     * @return lista de registros capturados durante la ventana de medición
     */
    private List<TransactionRecord> runMeasurement(Connection conn,
                                                    LoadProfile profile) throws SQLException {
        LOG.info("[BenchmarkRunner] Medición: " + measurementSecs + " s");
        List<TransactionRecord> samples = new ArrayList<>();
        long endMs = System.currentTimeMillis() + (long) measurementSecs * 1_000;

        while (System.currentTimeMillis() < endMs) {
            for (Map.Entry<String, LoadProfile.QueryStats> entry : profile.getQueries().entrySet()) {
                TransactionRecord record = queryExecutor.execute(
                        conn, entry.getKey(), entry.getValue(), null);
                samples.add(record);
            }
            sleep(thinkTimeMs);
        }

        LOG.info("[BenchmarkRunner] Medición completada: " + samples.size() + " muestras.");
        return samples;
    }

    // -------------------------------------------------------------------------
    // Construcción del resultado
    // -------------------------------------------------------------------------

    private BenchmarkResult buildResult(List<TransactionRecord> samples,
                                        LoadProfile profile,
                                        String commitSha) {
        Map<String, List<TransactionRecord>> byQuery = new HashMap<>();
        for (TransactionRecord r : samples) {
            byQuery.computeIfAbsent(r.getQueryId(), k -> new ArrayList<>()).add(r);
        }

        long windowMs   = Math.max((long) measurementSecs * 1_000, 1L);
        double windowMin = windowMs / 60_000.0;

        Map<String, BenchmarkResult.QueryResult> queryResults = new HashMap<>();
        boolean anyFail = false;

        for (Map.Entry<String, List<TransactionRecord>> entry : byQuery.entrySet()) {
            String qid = entry.getKey();
            List<TransactionRecord> qSamples = entry.getValue();

            List<Long> latencies = new ArrayList<>();
            double totalPlanCost = 0.0;
            for (TransactionRecord r : qSamples) {
                latencies.add(r.getLatencyMs());
                totalPlanCost += QueryExecutor.extractPlanCost(r.getExecutionPlan());
            }
            Collections.sort(latencies);

            long n        = latencies.size();
            double mean   = latencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
            double median = percentile(latencies, 50.0);
            double p95    = percentile(latencies, 95.0);
            double p99    = percentile(latencies, 99.0);
            long   min    = latencies.isEmpty() ? 0 : latencies.get(0);
            long   max    = latencies.isEmpty() ? 0 : latencies.get(latencies.size() - 1);
            double cpm    = n / windowMin;
            double avgPlanCost = n > 0 ? totalPlanCost / n : 0.0;

            // Veredicto basado en el umbral @Req del perfil de Fase 1
            LoadProfile.QueryStats stats = profile.getQueries().get(qid);
            BenchmarkResult.Verdict verdict = BenchmarkResult.Verdict.PASS;
            String failReason = null;

            // La comparación contra maxResponseTimeMs viene de QueryEntry (Fase 1)
            // Aquí usamos el p95 medido vs. el p95 del perfil de producción como proxy
            // (la comparación definitiva contra @Req la hace DegradationDetector)
            if (p95 > stats.getP95Ms() * 2) {
                verdict = BenchmarkResult.Verdict.FAIL;
                failReason = String.format("p95=%.0fms supera 2× el p95 de producción (%.0fms)",
                        p95, stats.getP95Ms());
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
                    .planCost(avgPlanCost)
                    .verdict(verdict)
                    .failReason(failReason)
                    .build());
        }

        return BenchmarkResult.builder()
                .profileName(profileName)
                .executedAt(Instant.now())
                .commitSha(commitSha)
                .totalOperations(samples.size())
                .queries(Collections.unmodifiableMap(queryResults))
                .overallVerdict(anyFail
                        ? BenchmarkResult.Verdict.FAIL
                        : BenchmarkResult.Verdict.PASS)
                .build();
    }

    // -------------------------------------------------------------------------

    private double percentile(List<Long> sorted, double p) {
        if (sorted.isEmpty()) return 0.0;
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
