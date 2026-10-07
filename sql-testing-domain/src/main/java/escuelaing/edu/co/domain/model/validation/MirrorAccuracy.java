package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

/**
 * Empirical comparison of mirror database latencies and cardinalities against the load
 * profile.
 *
 * <p>{@code status} is "PASS" when at least 90 % of the non-excluded latency checks and at
 * least 90 % of the cardinality checks pass. When {@code applicable=false} (nightly runs
 * with {@code testProfile=normal}) the check is skipped by design, since those runs measure
 * volume drift rather than mirror equivalence, and {@code status="N/A"}.</p>
 */
@Data
@Builder
public class MirrorAccuracy {

    /** {@code false} for nightly runs ({@code testProfile=normal}); status is then "N/A". */
    private boolean applicable;

    /**
     * Rows per table in the capture database ({@code loadtest.capture.rowsPerTable});
     * 0 when not configured.
     */
    private long captureRowsPerTable;

    /**
     * Rows per table generated in the mirror database ({@code loadtest.synthetic.rowsPerTable}).
     */
    private long mirrorRowsPerTable;

    private FidelitySummary latencySummary;
    private Map<String, LatencyFidelity>     latencyDetails;

    private FidelitySummary cardinalitySummary;
    private Map<String, CardinalityFidelity> cardinalityDetails;

    /** "PASS" / "FAIL" / "N/A". */
    private String status;
}
