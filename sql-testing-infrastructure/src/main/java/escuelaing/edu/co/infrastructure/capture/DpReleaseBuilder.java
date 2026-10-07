package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.DpRelease;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Computes the differentially private release ({@link DpRelease}) of the sanitized rows the
 * interceptor captured, and provides the Gaussian primitives reused by {@link TableProfiler}
 * and {@link FkDegreeProfiler}.
 *
 * <p>In this row-level release the privacy unit is a captured row; a record read several
 * times contributes several rows. {@link TableProfiler} gives record-level protection over
 * whole tables and is used instead when available.</p>
 *
 * <h3>Numeric columns (Gaussian mechanism)</h3>
 * For each numeric column with declared bounds [L, U], values are clipped to [L, U] and two
 * statistics are released: the mean (L2 sensitivity (U−L)/n) and the second moment
 * (sensitivity (max(L²,U²) − min c² over [L, U])/n), each with
 * σ = Δ·√(2·ln(1.25/δᵢ))/εᵢ (Dwork &amp; Roth 2014, Thm. A.1). ε_num is split evenly across
 * the 2·k numeric releases; δ is split across all numeric and categorical releases
 * (sequential composition). The released standard deviation is derived from the noisy
 * moments (post-processing).
 *
 * <h3>Categorical columns (thresholded Gaussian histogram)</h3>
 * For each column holding non-blank strings, value counts receive Gaussian noise with L2
 * sensitivity √2 (replacing one row moves one unit between two buckets). Buckets whose noisy
 * count does not exceed 1 + σ·√(2·ln(1/δᵢ)) are dropped, so rare values are not revealed.
 * ε_cat is split evenly across columns.
 *
 * <p>Noise is drawn from {@link SecureRandom} so the release cannot be reproduced from a
 * seed; benchmarks are reproducible because the released statistics are versioned.</p>
 */
public final class DpReleaseBuilder {

    static final String MECHANISM =
            "numeric: Gaussian mechanism on clipped mean and second moment (declared bounds); "
          + "categorical: thresholded Gaussian histogram; identifiers: never released";

    private final DpConfig config;
    private final Random   noise;

    public DpReleaseBuilder(DpConfig config) {
        this(config, new SecureRandom());
    }

    /** For tests only: allows a deterministic noise source. */
    DpReleaseBuilder(DpConfig config, Random noise) {
        this.config = config;
        this.noise  = noise;
    }

    /**
     * Gaussian-mechanism noise scale σ = Δ₂ · √(2 ln(1.25/δ)) / ε. The (ε, δ) guarantee holds
     * for 0 &lt; ε &lt; 1 (Dwork &amp; Roth 2014, Thm. A.1); larger ε is accepted but not covered.
     *
     * @throws IllegalArgumentException if {@code epsilon <= 0} or {@code delta} is not in (0, 1)
     */
    public static double gaussianSigma(double l2Sensitivity, double epsilon, double delta) {
        if (epsilon <= 0 || delta <= 0 || delta >= 1) {
            throw new IllegalArgumentException("Invalid (epsilon, delta)=(" + epsilon + ", " + delta + ")");
        }
        return l2Sensitivity * Math.sqrt(2.0 * Math.log(1.25 / delta)) / epsilon;
    }

    /**
     * Releases protected statistics of {@code rows} (column name to value; names are
     * lower-cased). Numbers are treated as numeric, non-blank strings as categorical; other
     * values are ignored. Numeric columns without declared bounds and categorical columns with
     * no surviving bucket are listed as suppressed.
     */
    public DpRelease build(List<Map<String, Object>> rows) {
        Map<String, List<Double>> numericValues = new TreeMap<>();
        Map<String, Map<String, Long>> stringCounts = new TreeMap<>();
        long sourceRows = 0;
        for (Map<String, Object> row : rows) {
            if (row == null || row.isEmpty()) continue;
            sourceRows++;
            for (Map.Entry<String, Object> e : row.entrySet()) {
                String col = e.getKey().toLowerCase();
                Object v = e.getValue();
                if (v instanceof Number n) {
                    numericValues.computeIfAbsent(col, k -> new ArrayList<>()).add(n.doubleValue());
                } else if (v instanceof String s && !s.isBlank()) {
                    stringCounts.computeIfAbsent(col, k -> new TreeMap<>()).merge(s, 1L, Long::sum);
                }
            }
        }

        List<String> suppressed = new ArrayList<>();
        List<String> releasable = new ArrayList<>();
        for (String col : numericValues.keySet()) {
            if (config.bounds().containsKey(col)) releasable.add(col);
            else suppressed.add(col);
        }

        Map<String, DpRelease.NumericColumn> numeric = new LinkedHashMap<>();
        if (!releasable.isEmpty()) {
            int releases = 2 * releasable.size();
            double eps = config.epsilonNumeric() / releases;
            double del = config.delta() / (releases + Math.max(1, stringCounts.size()));
            for (String col : releasable) {
                double[] b = config.bounds().get(col);
                numeric.put(col, releaseNumeric(numericValues.get(col), b[0], b[1], eps, del));
            }
        }

        Map<String, Map<String, Double>> categorical = new LinkedHashMap<>();
        if (!stringCounts.isEmpty()) {
            int cols = stringCounts.size();
            double eps = config.epsilonCategorical() / cols;
            double del = config.delta() / (2 * Math.max(1, releasable.size()) + cols);
            double sigma = gaussianSigma(Math.sqrt(2.0), eps, del);
            double threshold = 1.0 + sigma * Math.sqrt(2.0 * Math.log(1.0 / del));
            for (Map.Entry<String, Map<String, Long>> e : stringCounts.entrySet()) {
                Map<String, Double> released = releaseHistogram(e.getValue(), sigma, threshold);
                if (released.isEmpty()) suppressed.add(e.getKey());
                else categorical.put(e.getKey(), released);
            }
        }

        return DpRelease.builder()
                .mechanism(MECHANISM)
                .epsilonNumeric(config.epsilonNumeric())
                .epsilonCategorical(config.epsilonCategorical())
                .delta(config.delta())
                .sourceRows(sourceRows)
                .numeric(numeric)
                .categorical(categorical)
                .suppressedColumns(Collections.unmodifiableList(new ArrayList<>(new TreeSet<>(suppressed))))
                .build();
    }

    private DpRelease.NumericColumn releaseNumeric(List<Double> values, double lo, double hi,
                                                   double eps, double del) {
        int n = values.size();
        double sum = 0, sumSq = 0;
        for (double v : values) {
            double c = Math.max(lo, Math.min(hi, v));
            sum   += c;
            sumSq += c * c;
        }
        return releaseNumericMoments(n, sum, sumSq, lo, hi, eps, del);
    }

    /**
     * Thresholded Gaussian histogram: a value is released only if its noisy count exceeds
     * {@code threshold}, so rare values (potential identifiers) are never published.
     */
    Map<String, Double> releaseHistogram(Map<String, Long> counts, double sigma, double threshold) {
        Map<String, Double> released = new TreeMap<>();
        for (Map.Entry<String, Long> c : counts.entrySet()) {
            double noisy = c.getValue() + noise.nextGaussian() * sigma;
            if (noisy > threshold) released.put(c.getKey(), Math.rint(noisy));
        }
        return released;
    }

    /**
     * Gaussian release of a clipped mean and second moment from pre-aggregated sums
     * ({@code sum} and {@code sumSq} must already be computed over values clipped to
     * {@code [lo, hi]}). {@code n} is treated as public. Used for both row-level and
     * table-level releases. The noisy mean is clamped to {@code [lo, hi]} and the standard
     * deviation to {@code (hi − lo)/2}; if the noisy variance is not positive, a fallback of
     * {@code (hi − lo)/20} is released (post-processing, no extra budget).
     */
    DpRelease.NumericColumn releaseNumericMoments(long n, double sum, double sumSq,
                                                  double lo, double hi, double eps, double del) {
        double mean = sum / n;
        double m2   = sumSq / n;
        double maxSq = Math.max(lo * lo, hi * hi);
        double minSq = (lo <= 0 && hi >= 0) ? 0.0 : Math.min(lo * lo, hi * hi);
        double sigmaMean = gaussianSigma((hi - lo) / n, eps, del);
        double sigmaM2   = gaussianSigma((maxSq - minSq) / n, eps, del);
        double noisyMean = Math.max(lo, Math.min(hi, mean + noise.nextGaussian() * sigmaMean));
        double noisyM2   = Math.max(0.0, m2 + noise.nextGaussian() * sigmaM2);
        double variance  = noisyM2 - noisyMean * noisyMean;
        double maxStd    = (hi - lo) / 2.0;
        double std = variance > 0 ? Math.min(Math.sqrt(variance), maxStd) : maxStd / 10.0;
        return DpRelease.NumericColumn.builder()
                .lower(lo).upper(hi).n(n)
                .mean(noisyMean).std(std)
                .sigmaMean(sigmaMean).sigmaSecondMoment(sigmaM2)
                .build();
    }
}
