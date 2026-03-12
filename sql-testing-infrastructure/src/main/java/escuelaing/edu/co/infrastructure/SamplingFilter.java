package escuelaing.edu.co.infrastructure;

import escuelaing.edu.co.domain.model.QueryEntry;
import org.springframework.stereotype.Component;

import java.util.Random;

/**
 * Decide si una ejecución de consulta debe registrarse como
 * {@link escuelaing.edu.co.domain.model.TransactionRecord}.
 *
 * <h3>Reglas de muestreo — en orden de precedencia</h3>
 * <ol>
 *   <li><b>priority == HIGH</b> → siempre capturar.</li>
 *   <li><b>latencia &gt; maxResponseTimeMs</b> declarado en {@code @Req}
 *       → siempre capturar (anomalía de rendimiento).</li>
 *   <li>Ninguna regla anterior → capturar con probabilidad 0.10 (10 %).</li>
 * </ol>
 *
 * <p>Cuando el {@code queryId} no existe en el registro (código legacy sin
 * {@code @SqlQuery}), se aplica únicamente la regla probabilística.</p>
 */
@Component
public class SamplingFilter {

    private static final double DEFAULT_SAMPLE_RATE = 0.10;

    private final QueryRegistryLoader registryLoader;
    private final Random random;

    public SamplingFilter(QueryRegistryLoader registryLoader) {
        this.registryLoader = registryLoader;
        this.random = new Random();
    }

    /**
     * Evalúa si la ejecución identificada por {@code queryId} con la latencia
     * medida debe registrarse.
     *
     * @param queryId   identificador de la consulta (puede ser {@code null}
     *                  si no hay {@link CaptureContext} activo)
     * @param latencyMs latencia medida por el wrapper JDBC, en milisegundos
     * @return {@code true} si la transacción debe registrarse
     */
    public boolean shouldRecord(String queryId, long latencyMs) {
        if (queryId == null) {
            return false;
        }

        QueryEntry entry = registryLoader.get(queryId);

        if (entry != null) {
            // Regla 1: prioridad HIGH → siempre capturar
            if ("HIGH".equalsIgnoreCase(entry.getPriority())) {
                return true;
            }

            // Regla 2: anomalía de rendimiento → siempre capturar
            if (entry.isHasReq() && latencyMs > entry.getMaxResponseTimeMs()) {
                return true;
            }
        }

        // Regla 3: muestreo probabilístico al 10 %
        return random.nextDouble() < DEFAULT_SAMPLE_RATE;
    }
}
