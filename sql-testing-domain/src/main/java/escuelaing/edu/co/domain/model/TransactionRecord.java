package escuelaing.edu.co.domain.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

/**
 * Registro de una ejecución de consulta SQL capturada en producción.
 * Es el átomo de información que genera el wrapper JDBC (Fase 2)
 * y el insumo principal para construir el LoadProfile (Fase 3).
 */
@Data
@Builder
public class TransactionRecord {

    /** Identificador declarado en {@code @SqlQuery#queryId} — puente con Fase 1. */
    private String queryId;

    /** Texto SQL real ejecutado (con parámetros ya sustituidos si aplica). */
    private String sql;

    /** Latencia de ejecución medida por el wrapper JDBC, en milisegundos. */
    private long latencyMs;

    /** Momento exacto en que se inició la ejecución. */
    private Instant timestamp;

    /**
     * Plan de ejecución capturado con EXPLAIN ANALYZE, o {@code null}
     * cuando no se solicitó captura de plan para esta muestra.
     */
    private String executionPlan;
}
