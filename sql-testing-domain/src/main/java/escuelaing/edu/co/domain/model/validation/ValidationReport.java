package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Map;



/**
 * Informe consolidado de validación de fidelidad del generador de datos sintéticos.
 *
 * <p>Agrega los resultados de las tres dimensiones de validación definidas por
 * SynQB (Liu et al., 2024):</p>
 * <ol>
 *   <li><b>Fidelidad de latencia</b> — p95 sintético vs p95 real (error &lt; 10 %).</li>
 *   <li><b>Fidelidad de cardinalidad</b> — filas retornadas: calibrado vs no calibrado
 *       (error &lt; 5 %).</li>
 *   <li><b>Reproducibilidad</b> — misma semilla produce datos byte-identical.</li>
 * </ol>
 *
 * <p>Se persiste como {@code build/validation/VALIDATION_REPORT.json} al final
 * de cada ejecución de validación.</p>
 */
@Data
@Builder
public class ValidationReport {

    public enum Status { PASS, FAIL }

    /** Veredicto global: PASS si las tres dimensiones pasan. */
    private Status validationStatus;

    /** Momento en que se generó el informe. */
    private Instant generatedAt;

    /** Semilla del generador en esta ejecución. */
    private long seed;

    /** Parámetros del algoritmo DPSDG usados en esta generación. */
    private SyntheticDataInfo syntheticDataGeneration;

    /** Resumen de la dimensión de fidelidad de latencia. */
    private FidelitySummary latencySummary;

    /** Resultado de fidelidad de latencia por {@code queryId}. */
    private Map<String, LatencyFidelity> latencyFidelity;

    /** Resumen de la dimensión de fidelidad de cardinalidad. */
    private FidelitySummary cardinalitySummary;

    /** Resultado de fidelidad de cardinalidad por {@code queryId}. */
    private Map<String, CardinalityFidelity> cardinalityFidelity;

    /** Resultado de la verificación de reproducibilidad. */
    private ReproducibilityCheck reproducibility;

    /** {@code true} si {@code validationStatus == PASS}. */
    private boolean pass;

    /**
     * Nota legible sobre el resultado de la validación.
     * Ejemplo: "All dimensions pass threshold. Proceed with benchmark."
     * o "Cardinality fidelity FAIL: 2 queries exceeded 5% threshold."
     */
    private String notes;
}
