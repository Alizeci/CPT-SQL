package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * Cardinality fidelity result for a single write query.
 * Pass criterion: {@code errorPct < 5 %}.
 */
@Data
@Builder
public class CardinalityFidelity {

    private String queryId;
    private long rowsReal;
    private long rowsSynthetic;

    /** Relative error: {@code |rows_syn - rows_real| / rows_real × 100}. */
    private double errorPct;

    private boolean pass;
}
