package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * Privacy parameters of mirror data generation, for auditability. Budget composition:
 * {@code epsilonTotal = epsilonFt + epsilonUii} (sequential composition).
 *
 * <p>Not referenced by {@link ValidationReport}; {@link DataQuality} carries the privacy
 * parameters actually reported.</p>
 *
 * @deprecated Unused. {@link DataQuality} reports the privacy parameters. Scheduled for
 *             removal in a future release.
 */
@Deprecated(forRemoval = true)
@Data
@Builder
public class SyntheticDataInfo {

    private double epsilonFt;
    private double epsilonUii;
    private double epsilonTotal;

    /** δ parameter for the (ε, δ)-DP Gaussian mechanism. */
    private double delta;

    private long totalRowsGenerated;

    /** Human-readable schema coverage, e.g. {@code "100% (5 tables)"}. */
    private String schemaCoverage;
}
