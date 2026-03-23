package escuelaing.edu.co.domain.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Map;

/**
 * Perfil de carga de producción: resumen estadístico de las consultas
 * capturadas durante un período de observación.
 *
 * <p>Es el artefacto de salida de la Fase 2 y el insumo de la Fase 3
 * (motor de pruebas de carga). Contiene, por cada {@code queryId},
 * las métricas agregadas necesarias para sintetizar la carga.</p>
 */
@Data
@Builder
public class LoadProfile {

    /** Momento en que se generó el perfil. */
    private Instant generatedAt;

    /** Número total de transacciones que originaron este perfil. */
    private long totalSamples;

    /** Estadísticas por {@code queryId}. */
    private Map<String, QueryStats> queries;

    // -------------------------------------------------------------------------

    /**
     * Estadísticas agregadas para una consulta individual.
     */
    @Data
    @Builder
    public static class QueryStats {

        /** {@code queryId} al que pertenecen estas estadísticas. */
        private String queryId;

        /** Número de muestras que contribuyeron al cálculo. */
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

        /**
         * Frecuencia de llamadas estimada, en llamadas por minuto.
         * Se calcula dividiendo {@code sampleCount} entre la duración
         * del período de observación.
         */
        private double callsPerMinute;

        /**
         * Un ejemplo del SQL real capturado en producción para este queryId.
         * Usado por {@code QueryExecutor} en Fase 3 para ejecutar el query
         * real del usuario en lugar de un fallback sintético.
         * Puede ser null si no se capturó SQL concreto.
         */
        private String capturedSql;

        /**
         * Promedio de filas afectadas por ejecución (INSERT/UPDATE/DELETE).
         * {@code 0.0} si la query es SELECT o no se capturaron datos de cardinalidad.
         * Usado en la validación de fidelidad de cardinalidad (SynQB §3.5.1).
         */
        private double avgRowCount;
    }
}
