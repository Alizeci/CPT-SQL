package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * Data-quality part of the validation report. Always evaluated.
 *
 * <p>Reports (a) the privacy mode actually used to generate the mirror database and the
 * parameters of the differentially private release it was derived from, exactly as
 * recorded in the load profile, and (b) byte-identical reproducibility of the generator.
 * {@code status} only certifies reproducibility ("REPRODUCIBLE"); it says nothing about
 * privacy. Privacy fields are zero/empty in {@code LEGACY_UNPROTECTED} mode.</p>
 */
@Data
@Builder
public class DataQuality {

    /** "DP_RELEASE" when generated from a differentially private release; "LEGACY_UNPROTECTED" otherwise. */
    private String privacyMode;
    /** {@code epsilonNumeric + epsilonCategorical + epsilonForeignKey}. */
    private double epsilonTotal;
    /** Budget spent on numeric columns (Gaussian mechanism on clipped moments). */
    private double epsilonNumeric;
    /** Budget spent on categorical histograms. */
    private double epsilonCategorical;
    /** Budget spent on foreign-key concentration curves. */
    private double epsilonForeignKey;
    /** Total δ (attribute release + foreign-key release, basic composition). */
    private double delta;
    /** Foreign keys generated from a released concentration curve ({@code child_table.fk_column}). */
    private java.util.List<String> dpForeignKeys;
    /** {@link escuelaing.edu.co.domain.model.DpRelease#getSourceRows() Input size} of the release. */
    private long dpSourceRows;
    /** Numeric columns released (with declared bounds). */
    private java.util.List<String> dpReleasedColumns;
    /** Columns observed but suppressed (no declared bounds or below release threshold). */
    private java.util.List<String> dpSuppressedColumns;
    /**
     * True only if raw sanitized rows were inserted: no release and the explicit opt-in
     * {@code loadtest.synthetic.seedRealRows=true}.
     */
    private boolean realRowsSeeded;

    /** Mechanism applied to feature columns, as recorded in the release. */
    private String featureMechanism;

    /** Treatment of identifier columns. */
    private String identifierMechanism;

    private long   totalRowsGenerated;
    private String schemaCoverage;

    private ReproducibilityCheck reproducibility;

    /** "REPRODUCIBLE" when {@code reproducibility.byteIdentical == true}, otherwise "FAIL". */
    private String status;
}
