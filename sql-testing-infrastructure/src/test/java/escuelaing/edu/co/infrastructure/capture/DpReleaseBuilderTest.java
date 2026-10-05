package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.DpRelease;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class DpReleaseBuilderTest {

    private static List<Map<String, Object>> rows(int n) {
        List<Map<String, Object>> out = new ArrayList<>();
        Random r = new Random(1);
        for (int i = 0; i < n; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("price", 50 + r.nextDouble() * 100);          // true mean ≈ 100
            row.put("rating", 4.0);
            row.put("secret_amount", 1_000_000.0);                 // no declared bound
            row.put("category", i % 4 == 0 ? "home" : "books");    // 25 % / 75 %
            if (i == 0) row.put("status", "RARE_VALUE");           // must not be released
            out.add(row);
        }
        return out;
    }

    private static DpConfig config() {
        return new DpConfig(DpConfig.parseBounds("price:0:600,rating:0:5"), 0.6, 0.4, 1e-6, false);
    }

    @Test
    @DisplayName("gaussianSigma follows Δ·√(2 ln(1.25/δ))/ε")
    void gaussianSigma_formula() {
        double sigma = DpReleaseBuilder.gaussianSigma(1.0, 0.5, 1e-5);
        assertThat(sigma).isCloseTo(Math.sqrt(2 * Math.log(1.25 / 1e-5)) / 0.5, within(1e-9));
        assertThatThrownBy(() -> DpReleaseBuilder.gaussianSigma(1.0, 0.0, 1e-5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("numeric columns without declared bounds are suppressed, never released")
    void unboundedNumericColumnsAreSuppressed() {
        DpRelease release = new DpReleaseBuilder(config(), new Random(7)).build(rows(5000));
        assertThat(release.getNumeric()).containsOnlyKeys("price", "rating");
        assertThat(release.getSuppressedColumns()).contains("secret_amount");
    }

    @Test
    @DisplayName("released mean stays within declared bounds and close to the truth for large n")
    void releasedMeanIsBoundedAndUseful() {
        DpRelease release = new DpReleaseBuilder(config(), new Random(7)).build(rows(20000));
        DpRelease.NumericColumn price = release.getNumeric().get("price");
        assertThat(price.getMean()).isBetween(0.0, 600.0);
        assertThat(price.getMean()).isCloseTo(100.0, within(10.0));
        assertThat(price.getLower()).isEqualTo(0.0);
        assertThat(price.getUpper()).isEqualTo(600.0);
    }

    @Test
    @DisplayName("rare categorical values fall below the release threshold")
    void rareCategoriesAreNotReleased() {
        DpRelease release = new DpReleaseBuilder(config(), new Random(7)).build(rows(20000));
        assertThat(release.getCategorical().get("category")).containsKeys("books", "home");
        assertThat(release.getCategorical()).doesNotContainKey("status");
    }

    @Test
    @DisplayName("parseBounds rejects malformed or empty ranges")
    void parseBoundsValidates() {
        assertThatThrownBy(() -> DpConfig.parseBounds("price:10:5"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DpConfig.parseBounds("price:10"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(DpConfig.parseBounds("Price:0:600")).containsKey("price");
    }
}
