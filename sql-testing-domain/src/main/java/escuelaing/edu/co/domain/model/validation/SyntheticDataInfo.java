package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * Parámetros del algoritmo DPSDG usados en una ejecución de
 * {@code SyntheticDataGenerator}, incluidos en {@link ValidationReport}
 * para auditoría y reproducibilidad científica.
 *
 * <p>Refleja la composición secuencial del presupuesto de privacidad
 * (ε_total = ε_ft + ε_uii) definida en SynQB Theorem 4.4 (Liu et al., 2024).</p>
 */
@Data
@Builder
public class SyntheticDataInfo {

    /** Presupuesto ε para Feature columns (mecanismo Gaussiano). */
    private double epsilonFt;

    /** Presupuesto ε para UII columns (mecanismo Gaussiano). */
    private double epsilonUii;

    /** ε_total = ε_ft + ε_uii por composición secuencial. */
    private double epsilonTotal;

    /** Parámetro δ asumido para el mecanismo Gaussiano (ε, δ)-DP. */
    private double delta;

    /** Total de filas generadas en todas las tablas. */
    private long totalRowsGenerated;

    /**
     * Cobertura del schema en formato legible, e.g. {@code "100% (5 tables)"}.
     * Se calcula en {@code BenchmarkRunner.performValidation()} a partir de las
     * tablas descubiertas por {@code SyntheticDataGenerator}.
     */
    private String schemaCoverage;
}
