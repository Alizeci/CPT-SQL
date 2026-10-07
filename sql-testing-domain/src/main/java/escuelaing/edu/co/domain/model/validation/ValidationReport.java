package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * Fidelity validation report: how close the mirror database is to the capture environment.
 *
 * <ul>
 *   <li>{@link DataQuality}: privacy-budget metadata of the differentially private release
 *       and byte-identical reproducibility. Always evaluated.</li>
 *   <li>{@link MirrorAccuracy}: empirical latency and cardinality equivalence.
 *       Skipped ({@code status="N/A"}) for nightly runs using the {@code normal} profile.</li>
 * </ul>
 *
 * <p>Persisted as {@code VALIDATION_REPORT.json}.</p>
 */
@Data
@Builder
public class ValidationReport {

    /**
     * Overall verdict: "PASS" when {@code dataQuality} is REPRODUCIBLE and
     * {@code mirrorAccuracy} is PASS or not applicable; "FAIL" otherwise.
     */
    private String validationStatus;

    private String generatedAt;

    private DataQuality    dataQuality;
    private MirrorAccuracy mirrorAccuracy;

    /** {@code false} only when {@code validationStatus == "FAIL"}. */
    public boolean isPass() {
        return !"FAIL".equals(validationStatus);
    }
}
