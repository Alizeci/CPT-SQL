package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * Resultado de la validación de fidelidad de cardinalidad para una consulta individual.
 *
 * <p>Compara el número de filas retornadas por cada consulta entre la BD espejo
 * calibrada con el {@code LoadProfile} (proxy de "real") y la BD espejo con datos
 * puramente sintéticos sin calibración. Un error menor al 5 % confirma que la
 * calibración por {@code callsPerMinute} produce una distribución de cardinalidad
 * coherente con los patrones de producción.</p>
 *
 * <p>Criterio de aceptación: {@code errorPct < 5 %} — alineado con la dimensión de
 * fidelidad estructural de SynQB (Liu et al., 2024).</p>
 */
@Data
@Builder
public class CardinalityFidelity {

    /** Identificador de la consulta evaluada. */
    private String queryId;

    /** Filas retornadas sobre la BD espejo calibrada con el {@code LoadProfile}. */
    private long rowsReal;

    /** Filas retornadas sobre la BD espejo con datos no calibrados. */
    private long rowsSynthetic;

    /** Error relativo: {@code |rows_syn - rows_real| / rows_real × 100}. */
    private double errorPct;

    /** {@code true} si {@code errorPct < 5 %}. */
    private boolean pass;
}
