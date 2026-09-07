package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * Two-layer validation report for the CPT-SQL benchmark engine.
 *
 * <ul>
 *   <li>{@link DataQuality} — algorithmic DPSDG guarantee (Liu et al. 2024):
 *       DP-budget metadata and byte-identical reproducibility. Always evaluated.</li>
 *   <li>{@link MirrorAccuracy} — empirical latency and cardinality equivalence.
 *       Skipped ({@code status="N/A"}) for nightly runs using the {@code normal} profile.</li>
 * </ul>
 *
 * <p>Persisted as {@code VALIDATION_REPORT.json}.</p>
 */
@Data
@Builder
public class ValidationReport {

    /** Overall verdict: "PASS", "FAIL", or "N/A". */
    private String validationStatus;

    private String generatedAt;

    private DataQuality    dataQuality;
    private MirrorAccuracy mirrorAccuracy;

    /** {@code false} only when {@code validationStatus == "FAIL"}. */
    public boolean isPass() {
        return !"FAIL".equals(validationStatus);
    }
}
