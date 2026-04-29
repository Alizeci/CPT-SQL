package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * DPSDG parameters used during synthetic data generation, included in
 * {@link ValidationReport} for scientific auditability.
 * Budget composition: ε_total = ε_ft + ε_uii (sequential composition).
 */
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
