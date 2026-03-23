package escuelaing.edu.co.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Resultado de una ejecución del motor de pruebas de carga (Fase 3).
 *
 * <p>Contiene, por cada {@code queryId}, las métricas medidas durante el
 * benchmark y el veredicto {@code PASS/FAIL} comparado contra los umbrales
 * declarados en {@code @Req} y la línea base almacenada.</p>
 *
 * <p>Se persiste como artefacto JSON con nombre
 * {@code benchmark-<profileName>-YYYYMMDD-HHmmss.json} (Fase 4).</p>
 *
 * <h3>Métricas enriquecidas</h3>
 * <ul>
 *   <li>{@code slaComplianceRate} por query: porcentaje de operaciones que
 *       no violaron el SLA — métrica M de Dyn-YCSB (Sidhanta et al., 2019).</li>
 *   <li>{@code throughputTimeSeries}: TPS observado cada 10 s durante la
 *       ventana de medición — visualización en tiempo real de BenchPress
 *       (Van Aken et al., SIGMOD 2015 §4.2).</li>
 *   <li>{@code latencyTimeSeries} por query: p50/p95/p99 por ventana de 10 s.</li>
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BenchmarkResult {

    /** Nombre del perfil de carga ejecutado (ej. "pre-test", "nightly"). */
    private String profileName;

    /** Nombre del {@link TestProfile} utilizado (ej. "light", "peak"). */
    private String testProfileName;

    /** Momento en que se completó el benchmark. */
    private Instant executedAt;

    /** SHA del commit que disparó la ejecución (puede ser null en ejecuciones manuales). */
    private String commitSha;

    /** Total de operaciones ejecutadas durante la ventana de medición. */
    private long totalOperations;

    /** Resultados por {@code queryId}. */
    private Map<String, QueryResult> queries;

    /** Veredicto global: PASS si todas las consultas pasaron, FAIL si alguna falló. */
    private Verdict overallVerdict;

    /**
     * TPS observado cada {@code metricsWindowSecs} segundos durante la
     * ventana de medición.  Permite visualizar la evolución del throughput
     * (BenchPress §4.2 Performance Visualization).
     */
    private List<ThroughputSnapshot> throughputTimeSeries;

    /**
     * Pico máximo de throughput alcanzado durante el benchmark, en TPS.
     * Relevante en perfiles {@code OPEN_LOOP} y en fases de pico.
     */
    private double peakThroughputAchieved;

    // -------------------------------------------------------------------------

    /** Veredicto de una ejecución de benchmark. */
    public enum Verdict { PASS, FAIL }

    // -------------------------------------------------------------------------

    /**
     * Snapshot de throughput para la serie de tiempo.
     * Capturado cada {@code metricsWindowSecs} segundos (default 10 s).
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ThroughputSnapshot {

        /** Milisegundos transcurridos desde el inicio de la ventana de medición. */
        private long elapsedMs;

        /** Transacciones por segundo observadas en esta ventana. */
        private double tps;

        /** Fase del TestProfile activa en este momento. */
        private String phaseName;
    }

    /**
     * Snapshot de latencias para la serie de tiempo por query.
     * Permite ver cómo evoluciona el p95 durante las fases de carga.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LatencySnapshot {

        /** Milisegundos transcurridos desde el inicio de la ventana de medición. */
        private long elapsedMs;

        /** Latencia mediana en esta ventana, en milisegundos. */
        private double p50Ms;

        /** Percentil 95 en esta ventana, en milisegundos. */
        private double p95Ms;

        /** Percentil 99 en esta ventana, en milisegundos. */
        private double p99Ms;

        /** Fase del TestProfile activa en este momento. */
        private String phaseName;
    }

    /**
     * Métricas y veredicto para una consulta individual dentro del benchmark.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class QueryResult {

        /** Identificador de la consulta — puente con Fase 1. */
        private String queryId;

        /** Número de ejecuciones realizadas durante la ventana de medición. */
        private long sampleCount;

        /** Latencia media, en milisegundos. */
        private double meanMs;

        /** Latencia mediana (p50), en milisegundos. */
        private double medianMs;

        /** Percentil 95 de latencia, en milisegundos. */
        private double p95Ms;

        /** Percentil 99 de latencia, en milisegundos. */
        private double p99Ms;

        /** Latencia mínima observada, en milisegundos. */
        private long minMs;

        /** Latencia máxima observada, en milisegundos. */
        private long maxMs;

        /** Frecuencia de ejecución durante el benchmark, en llamadas por minuto. */
        private double callsPerMinute;

        /**
         * Costo estimado del planificador capturado con {@code EXPLAIN ANALYZE}.
         * Permite comparar planes entre versiones del esquema o estadísticas.
         */
        private double planCost;

        /**
         * Porcentaje de operaciones que NO violaron el SLA declarado en {@code @Req}.
         *
         * <p>Equivale a la métrica M de Dyn-YCSB (Sidhanta et al., 2019):
         * "computes the percentage of operations which did not violate the SLA."
         * Un valor de 100.0 significa que todas las operaciones estuvieron dentro
         * del umbral {@code maxResponseTimeMs}.</p>
         */
        private double slaComplianceRate;

        /**
         * Porcentaje del umbral SLA consumido por el p95 medido.
         * Ejemplo: p95=86ms con SLA=100ms → slaRiskPct=86%.
         * Valores > 70% indican riesgo de violación bajo mayor carga.
         */
        private double slaRiskPct;

        /**
         * Evolución de latencias por ventana de tiempo durante la medición.
         * Permite detectar degradaciones puntuales dentro de la ventana
         * (BenchPress §4.2 Performance Visualization).
         */
        private List<LatencySnapshot> latencyTimeSeries;

        /** Veredicto individual: PASS o FAIL. */
        private Verdict verdict;

        /**
         * Razón del fallo cuando {@code verdict == FAIL}, {@code null} cuando PASS.
         * Ejemplo: "p95=320ms supera maxResponseTimeMs=200ms".
         */
        private String failReason;
    }
}
