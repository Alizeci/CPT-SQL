package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * Resultado de la validación de fidelidad de latencia para una consulta individual.
 *
 * <p>Compara el p95 medido en producción (del {@code LoadProfile}) contra el p95
 * obtenido ejecutando la misma consulta sobre los datos sintéticos de la BD espejo.
 * Un error menor al 10 % indica que los datos sintéticos reproducen fielmente el
 * comportamiento de latencia observado en producción.</p>
 *
 * <p>Criterio de aceptación: {@code errorPct < 10 %} — equivalente al 1 % de error
 * de latencia validado por OLTP-Bench (Difallah et al., VLDB 2013) y alineado con
 * la dimensión de fidelidad de SynQB (Liu et al., 2024).</p>
 */
@Data
@Builder
public class LatencyFidelity {

    /** Identificador de la consulta evaluada. */
    private String queryId;

    /** p95 de latencia medido en producción (fuente: {@code LoadProfile.QueryStats}), en ms. */
    private long p95RealMs;

    /** p95 de latencia medido sobre la BD espejo con datos sintéticos, en ms. */
    private long p95SyntheticMs;

    /** Error relativo: {@code |p95_syn - p95_real| / p95_real × 100}. */
    private double errorPct;

    /** {@code true} si {@code errorPct < 10 %}. */
    private boolean pass;
}
