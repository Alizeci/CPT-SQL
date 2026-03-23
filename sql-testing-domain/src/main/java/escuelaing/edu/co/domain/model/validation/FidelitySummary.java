package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * Resumen agregado de una dimensión de fidelidad del generador de datos sintéticos.
 *
 * <p>Complementa los mapas de detalle ({@link LatencyFidelity}, {@link CardinalityFidelity})
 * con métricas de resumen que permiten evaluar rápidamente el estado global de una
 * dimensión sin iterar sobre cada query individual.</p>
 *
 * <p>Se incluye en {@link ValidationReport} para que el VALIDATION_REPORT.json
 * sea auto-descriptivo y auditable (§3.6.1).</p>
 */
@Data
@Builder
public class FidelitySummary {

    /** Número de queries evaluadas en esta dimensión. */
    private int queriesTested;

    /** Número de queries que superaron el umbral de aceptación. */
    private int queriesPassed;

    /** Error promedio (%) sobre todas las queries evaluadas. */
    private double meanErrorPct;
}
