package escuelaing.edu.co.domain.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Map;

/**
 * Resultado de una ejecución del motor de pruebas de carga (Fase 3).
 *
 * <p>Contiene, por cada {@code queryId}, las métricas medidas durante el
 * benchmark y el veredicto {@code PASS/FAIL} comparado contra los umbrales
 * declarados en {@code @Req} y la línea base almacenada.</p>
 *
 * <p>Se persiste como artefacto JSON con nombre
 * {@code benchmark-<profileName>-YYYYMMDD-HHmmss.json}.</p>
 */
@Data
@Builder
public class BenchmarkResult {

    /** Nombre del perfil de carga ejecutado (ej. "pre-test", "nightly"). */
    private String profileName;

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

    // -------------------------------------------------------------------------

    /** Veredicto de una ejecución de benchmark. */
    public enum Verdict { PASS, FAIL }

    /**
     * Métricas y veredicto para una consulta individual dentro del benchmark.
     */
    @Data
    @Builder
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

        /** Veredicto individual: PASS o FAIL. */
        private Verdict verdict;

        /**
         * Razón del fallo cuando {@code verdict == FAIL}, {@code null} cuando PASS.
         * Ejemplo: "p95=320ms supera maxResponseTimeMs=200ms".
         */
        private String failReason;
    }
}
