package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * Algorithmic guarantee from the DPSDG generator (Liu et al. 2024).
 * Covers DP-budget metadata and byte-identical reproducibility.
 * Always evaluated; {@code status} is "GUARANTEED" when {@code reproducibility.byteIdentical == true}.
 */
@Data
@Builder
public class DataQuality {

    private double epsilonTotal;
    private double epsilonFt;
    private double epsilonUii;
    private double delta;

    /** DP mechanism applied to FEATURE columns (numeric/categorical). */
    private String featureMechanism;

    /** DP mechanism applied to IDENTIFIER columns (high-cardinality keys). */
    private String identifierMechanism;

    private long   totalRowsGenerated;
    private String schemaCoverage;

    private ReproducibilityCheck reproducibility;

    /** "GUARANTEED" when {@code reproducibility.byteIdentical == true}, otherwise "FAIL". */
    private String status;
}
