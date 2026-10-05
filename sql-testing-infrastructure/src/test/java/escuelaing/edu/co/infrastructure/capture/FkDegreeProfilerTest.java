package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.DpRelease;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class FkDegreeProfilerTest {

    /** Noise source that adds nothing — isolates the deterministic part of the release. */
    private static final class NoNoise extends Random {
        @Override public synchronized double nextGaussian() { return 0.0; }
    }

    /** Zipf-like degrees: parent i (1-based) owns ≈ C / i children. */
    private static long[] zipfDegrees(int parents, long top) {
        long[] d = new long[parents];
        for (int i = 0; i < parents; i++) d[i] = Math.max(1, top / (i + 1));
        return d; // already descending
    }

    @Test
    @DisplayName("without noise the released curve equals the exact concentration curve")
    void release_exactWithoutNoise() {
        long[] deg = zipfDegrees(100_000, 50_000);
        long total = Arrays.stream(deg).sum();
        long top1pct = Arrays.stream(deg, 0, 1_000).sum();

        DpRelease.ForeignKeyDegree fk = FkDegreeProfiler.release(
                "order_items", "product_id", "products", deg, 100_000, 0.1, 1e-7, new NoNoise());

        assertThat(fk.getRankFractions()).last().isEqualTo(1.0);
        assertThat(fk.getCumulativeShare()).last().isEqualTo(1.0);
        assertThat(FkDegreeProfiler.shareAt(fk, 1e-2)).isCloseTo((double) top1pct / total, within(1e-9));
    }

    @Test
    @DisplayName("σ uses L2 sensitivity √m for the m released cumulative sums")
    void release_sigmaMatchesSensitivity() {
        long[] deg = zipfDegrees(400_000, 5_000);
        DpRelease.ForeignKeyDegree fk = FkDegreeProfiler.release(
                "c", "p_id", "p", deg, 400_000, 0.1, 2.5e-7, new NoNoise());
        int m = fk.getRankFractions().size();
        assertThat(m).isEqualTo(6);
        assertThat(fk.getSigma()).isCloseTo(
                DpReleaseBuilder.gaussianSigma(Math.sqrt(m), 0.1, 2.5e-7), within(1e-9));
    }

    @Test
    @DisplayName("post-processing always yields a non-decreasing, concave curve ending at 1")
    void release_validCurveUnderHeavyNoise() {
        long[] deg = zipfDegrees(2_000, 30);
        Random rnd = new Random(7);
        for (int run = 0; run < 50; run++) {
            DpRelease.ForeignKeyDegree fk = FkDegreeProfiler.release(
                    "c", "p_id", "p", deg, 2_000, 0.05, 1e-7, rnd);
            List<Double> s = fk.getCumulativeShare();
            List<Double> f = fk.getRankFractions();
            assertThat(s.get(s.size() - 1)).isEqualTo(1.0);
            double prevS = 0, prevF = 0, prevSlope = Double.POSITIVE_INFINITY;
            for (int i = 0; i < s.size(); i++) {
                assertThat(s.get(i)).isBetween(prevS - 1e-12, 1.0 + 1e-12);
                double slope = (s.get(i) - prevS) / (f.get(i) - prevF);
                assertThat(slope).isLessThanOrEqualTo(prevSlope + 1e-9);
                prevSlope = slope; prevS = s.get(i); prevF = f.get(i);
            }
        }
    }

    @Test
    @DisplayName("tiny parent tables collapse duplicate checkpoints")
    void release_tinyParentTable() {
        DpRelease.ForeignKeyDegree fk = FkDegreeProfiler.release(
                "c", "p_id", "p", new long[]{5, 3, 1}, 3, 0.1, 1e-7, new NoNoise());
        assertThat(fk.getRankFractions()).containsExactly(1.0 / 3, 1.0);
        assertThat(fk.getCumulativeShare().get(0)).isCloseTo(5.0 / 9, within(1e-9));
    }

    @Test
    @DisplayName("pool-adjacent-violators produces a non-increasing weighted fit")
    void nonIncreasing_pava() {
        double[] fit = FkDegreeProfiler.nonIncreasing(new double[]{5, 1, 3, 0}, new double[]{1, 1, 1, 1});
        assertThat(fit).containsExactly(5, 2, 2, 0);
    }
}
