package escuelaing.edu.co.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Differentially-private summary of the sanitized business columns observed during
 * Phase 2 capture.
 *
 * <p>This is the only information about production values that leaves the capture
 * environment: raw sanitized rows are discarded after the release is computed
 * (unless explicitly retained for debugging). Numeric columns are released as a
 * clipped mean and standard deviation using the Gaussian mechanism with declared
 * (data-independent) bounds; categorical columns are released as a thresholded
 * noisy histogram. Downstream consumers (synthetic data generator, parameter
 * binding) only post-process this release, so they consume no extra budget.</p>
 *
 * <p>Neighbouring datasets differ in one captured row (record-level DP, bounded
 * neighbour relation with public {@code n}). User-level protection is out of scope.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DpRelease {

    /** Human-readable mechanism description, persisted for auditability. */
    private String mechanism;

    /** Total budget spent on numeric columns (split evenly across mean + second moment of each column). */
    private double epsilonNumeric;

    /** Total budget spent on categorical histograms (split evenly across columns). */
    private double epsilonCategorical;

    /** Total δ (split evenly across all releases). */
    private double delta;

    /** Number of sanitized rows used as input (treated as public). */
    private long sourceRows;

    /** Released statistics per numeric column. */
    private Map<String, NumericColumn> numeric;

    /** Released (noisy, thresholded) counts per value, per categorical column. */
    private Map<String, Map<String, Double>> categorical;

    /** Columns observed but not released (e.g. numeric without declared bounds). */
    private java.util.List<String> suppressedColumns;

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
}
