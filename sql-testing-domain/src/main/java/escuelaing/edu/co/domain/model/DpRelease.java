package escuelaing.edu.co.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Differentially private release of the business columns of the capture environment,
 * computed either over the whole capture tables or over the sanitized rows read during
 * capture, plus optional foreign-key concentration curves.
 *
 * <p>This is the only information about production values that leaves the capture
 * environment: raw sanitized rows are discarded after the release is computed
 * (unless explicitly retained for debugging). Numeric columns are released as a
 * clipped mean and standard deviation using the Gaussian mechanism with declared
 * (data-independent) bounds; categorical columns are released as a thresholded
 * noisy histogram. Downstream consumers (mirror data generation, parameter binding)
 * only post-process this release, so they consume no extra budget.</p>
 *
 * <p>Neighbouring datasets differ in one row (record-level DP, bounded neighbour relation
 * with public {@code n}). User-level protection is out of scope.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DpRelease {

    /** Human-readable mechanism description, persisted for auditability. */
    private String mechanism;

    /** Total ε spent on numeric columns (split evenly across the mean and second moment of each column). */
    private double epsilonNumeric;

    /** Total ε spent on categorical histograms (split evenly across columns). */
    private double epsilonCategorical;

    /** δ of the numeric and categorical releases (split evenly across them). */
    private double delta;

    /**
     * Input size, treated as public: the number of sanitized rows for a release over captured
     * rows, or the largest per-column row count for a release over whole tables.
     */
    private long sourceRows;

    /** Released statistics per numeric column. */
    private Map<String, NumericColumn> numeric;

    /** Released (noisy, thresholded) counts per value, per categorical column. */
    private Map<String, Map<String, Double>> categorical;

    /**
     * Columns observed but not released: numeric columns without declared bounds, or
     * categorical columns whose every value fell below the release threshold.
     */
    private java.util.List<String> suppressedColumns;

    /**
     * Total ε spent on foreign-key concentration curves (split evenly across relations).
     * Zero when no relation was released.
     */
    private double epsilonForeignKey;

    /** δ spent on foreign-key concentration curves (split evenly across relations). */
    private double deltaForeignKey;

    /**
     * Released foreign-key concentration curve of each relation, keyed {@code child_table.fk_column}.
     * Captures how child rows concentrate on parent rows (e.g. a few best-selling products
     * own most order items), which per-column statistics cannot express.
     */
    private Map<String, ForeignKeyDegree> foreignKeys;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NumericColumn {
        /** Declared lower bound (clipping); public, not derived from data. */
        private double lower;
        /** Declared upper bound (clipping); public, not derived from data. */
        private double upper;
        /** Number of values for this column (public). */
        private long   n;
        /** Released (noisy) mean, clamped to [lower, upper]. */
        private double mean;
        /** Released standard deviation derived from noisy first and second moments. */
        private double std;
        /** Noise scale applied to the mean. */
        private double sigmaMean;
        /** Noise scale applied to the second moment. */
        private double sigmaSecondMoment;
    }

    /**
     * Foreign-key concentration curve: differentially private summary of the degree
     * distribution of one foreign key, i.e. the share of child rows owned by the top
     * {@code q} fraction of parent rows, for a fixed (data-independent) grid of fractions.
     *
     * <p>Neighbouring datasets differ in one child row. Adding or removing a child row
     * changes exactly one entry of the sorted degree sequence by one (Hay et al., ICDM
     * 2009), so each cumulative sum at a fixed rank changes by at most one and the vector
     * of {@code m} cumulative sums has L2 sensitivity {@code √m}. Parent identities are
     * never released: only the shape of the sorted sequence.</p>
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ForeignKeyDegree {
        private String childTable;
        private String column;
        private String parentTable;
        /** Fixed grid of parent-rank fractions (public), ascending, last = 1.0. */
        private java.util.List<Double> rankFractions;
        /**
         * Released share of child rows owned by the top {@code rankFractions[i]} parents,
         * after post-processing (monotone, concave, last = 1.0).
         */
        private java.util.List<Double> cumulativeShare;
        /** Noise scale applied to each cumulative count. */
        private double sigma;
    }
}
