package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * Data-quality layer of the validation report.
 *
 * <p>Reports (a) the privacy mode actually used to generate the mirror and the
 * parameters of the DP release it was derived from, exactly as recorded in the load
 * profile, and (b) byte-identical reproducibility of the generator. {@code status} only
 * certifies reproducibility ("REPRODUCIBLE"); it says nothing about privacy.</p>
 */
@Data
@Builder
public class DataQuality {

    /** "DP_RELEASE" when the mirror was generated from a DP release; "LEGACY_UNPROTECTED" otherwise. */
    private String privacyMode;
    private double epsilonTotal;
    /** Budget spent on numeric columns (Gaussian mechanism on clipped moments). */
    private double epsilonNumeric;
    /** Budget spent on categorical histograms. */
    private double epsilonCategorical;
    /** Budget spent on foreign-key degree shapes. */
    private double epsilonForeignKey;
    /** Total δ (attribute release + foreign-key release, basic composition). */
    private double delta;
    /** Foreign keys generated from the released degree shape (child_table.fk_column). */
    private java.util.List<String> dpForeignKeys;
    /** Number of sanitized rows summarised by the release. */
    private long dpSourceRows;
    /** Numeric columns released (with declared bounds). */
    private java.util.List<String> dpReleasedColumns;
    /** Columns observed but suppressed (no declared bounds or below release threshold). */
    private java.util.List<String> dpSuppressedColumns;
    /** True only if raw sanitized rows were inserted (legacy opt-in). */
    private boolean realRowsSeeded;

    /** Mechanism applied to feature columns, as recorded in the DP release. */
    private String featureMechanism;

    /** Treatment of identifier columns. */
    private String identifierMechanism;

    private long   totalRowsGenerated;
    private String schemaCoverage;

    private ReproducibilityCheck reproducibility;

    /** "REPRODUCIBLE" when {@code reproducibility.byteIdentical == true}, otherwise "FAIL". */
    private String status;
}
