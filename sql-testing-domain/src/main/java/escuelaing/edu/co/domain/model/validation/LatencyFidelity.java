package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * Latency fidelity result for a single query.
 * Pass criterion: {@code errorPct < 10 %}.
 */
@Data
@Builder
public class LatencyFidelity {

    private String queryId;
    private long p95RealMs;
    private long p95SyntheticMs;

    /** Relative error: {@code |p95_syn - p95_real| / p95_real × 100}. */
    private double errorPct;

    private boolean pass;
}
