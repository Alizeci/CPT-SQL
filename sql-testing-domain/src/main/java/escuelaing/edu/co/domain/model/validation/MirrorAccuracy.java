package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

/**
 * Empirical comparison of mirror-database latencies and cardinalities against
 * the production capture profile (Capa B).
 *
 * <p>When {@code applicable=false} (nightly runs), the check is skipped by design:
 * nightly captures volume drift rather than mirror equivalence, so {@code status="N/A"}.</p>
 */
@Data
@Builder
public class MirrorAccuracy {

    /** {@code false} for nightly ({@code testProfile=normal}) — status will be "N/A". */
    private boolean applicable;

    /** Total samples in the production capture profile (Phase 2). */
    private long captureVolume;

    /** Rows generated in the mirror database for this validation run. */
    private long mirrorVolume;

    private FidelitySummary latencySummary;
    private Map<String, LatencyFidelity>     latencyDetails;

    private FidelitySummary cardinalitySummary;
    private Map<String, CardinalityFidelity> cardinalityDetails;

    /** "PASS" / "FAIL" / "N/A". */
    private String status;
}
