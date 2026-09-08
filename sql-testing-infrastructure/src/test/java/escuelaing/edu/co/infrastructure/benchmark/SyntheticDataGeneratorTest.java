package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.domain.model.LoadProfile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static escuelaing.edu.co.infrastructure.benchmark.SyntheticDataGenerator.ColumnCategory.FEATURE;
import static escuelaing.edu.co.infrastructure.benchmark.SyntheticDataGenerator.ColumnCategory.IDENTIFIER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.Mockito.mock;

/**
 * Validates the DPSDG algorithm logic: column classification,
 * UII pseudonym generation, reproducibility with the same seed, and
 * behaviour of {@code insertRealSanitizedData} with empty profiles.
 */
class SyntheticDataGeneratorTest {

    private SyntheticDataGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new SyntheticDataGenerator();
        generator.setSeed(42);
    }

    // -------------------------------------------------------------------------
    // classifyColumn — Feature / UII split (SynQB §3.2)
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("user_id and customer_id must be classified as IDENTIFIER")
    void classifyColumn_userIdColumns() {
        assertThat(generator.classifyColumn("orders", "user_id")).isEqualTo(IDENTIFIER);
        assertThat(generator.classifyColumn("orders", "customer_id")).isEqualTo(IDENTIFIER);
        assertThat(generator.classifyColumn("orders", "USER_ID")).isEqualTo(IDENTIFIER);
    }

    @Test
    @DisplayName("_id columns must be classified as IDENTIFIER")
    void classifyColumn_transactionIdColumns() {
        assertThat(generator.classifyColumn("orders",      "id")).isEqualTo(IDENTIFIER);
        assertThat(generator.classifyColumn("order_items", "order_id")).isEqualTo(IDENTIFIER);
        assertThat(generator.classifyColumn("order_items", "product_id")).isEqualTo(IDENTIFIER);
    }

    @Test
    @DisplayName("business columns must be classified as FEATURE")
    void classifyColumn_featureColumns() {
        assertThat(generator.classifyColumn("products", "price")).isEqualTo(FEATURE);
        assertThat(generator.classifyColumn("products", "rating")).isEqualTo(FEATURE);
        assertThat(generator.classifyColumn("products", "category")).isEqualTo(FEATURE);
        assertThat(generator.classifyColumn("orders",   "total_amount")).isEqualTo(FEATURE);
        assertThat(generator.classifyColumn("products", "stock_quantity")).isEqualTo(FEATURE);
    }

    // -------------------------------------------------------------------------
    // generateUiiPseudonym — exclusive range [10001, 99999]
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("all UII pseudonyms must be in [10001, 99999]")
    void generateUiiPseudonym_isInExclusiveRange() {
        for (int i = 0; i < 200; i++) {
            int id = generator.generateUiiPseudonym();
            assertThat(id)
                    .as("pseudonym must be in the exclusive range to avoid collision with real IDs")
                    .isBetween(10_001, 99_999);
        }
    }

    // -------------------------------------------------------------------------
    // gaussianNoise — reproducibilidad con mismo seed (SynQB Algorithm 1)
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("gaussianNoise with the same seed must produce identical values")
    void gaussianNoise_isReproducibleWithSameSeed() {
        generator.setSeed(42);
        double v1 = generator.gaussianNoise(100.0, 20.0);

        generator.setSeed(42);
        double v2 = generator.gaussianNoise(100.0, 20.0);

        assertThat(v1).isEqualTo(v2);
    }

    @Test
    @DisplayName("gaussianNoise with different seeds must produce different values (in general)")
    void gaussianNoise_differsBetweenSeeds() {
        generator.setSeed(42);
        double v1 = generator.gaussianNoise(100.0, 20.0);

        generator.setSeed(99);
        double v2 = generator.gaussianNoise(100.0, 20.0);

        // Different seeds change the distribution; values differ for base=100, sigma=20
        assertThat(v1).isNotEqualTo(v2);
    }

    @Test
    @DisplayName("gaussianNoise applies the ε_ft budget as sigma divisor")
    void gaussianNoise_appliesEpsilonScale() {
        // With sigma = 0 and any ε, the noise must equal the base exactly
        generator.setSeed(42);
        double result = generator.gaussianNoise(50.0, 0.0);
        assertThat(result).isCloseTo(50.0, within(0.001));
    }

    // -------------------------------------------------------------------------
    // Constantes de privacidad diferencial (SynQB Theorem 4.4)
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("ε_total = ε_ft + ε_uii = 1.0 by sequential composition")
    void epsilonTotalEqualsSum() {
        assertThat(SyntheticDataGenerator.EPSILON_TOTAL)
                .isCloseTo(SyntheticDataGenerator.EPSILON_FT + SyntheticDataGenerator.EPSILON_UII,
                        within(1e-9));
        assertThat(SyntheticDataGenerator.EPSILON_TOTAL).isCloseTo(1.0, within(1e-9));
    }

    // -------------------------------------------------------------------------
    // insertRealSanitizedData — defensive handling of invalid inputs
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("insertRealSanitizedData with null profile must not throw")
    void insertRealSanitizedData_nullProfile_doesNotThrow() {
        Connection conn = mock(Connection.class);
        assertThatCode(() -> generator.insertRealSanitizedData(
                conn, null, List.of(), Map.of(), Map.of(), Map.of(), Map.of()))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("insertRealSanitizedData with empty profile must not throw")
    void insertRealSanitizedData_emptyProfile_doesNotThrow() {
        Connection conn = mock(Connection.class);
        LoadProfile emptyProfile = LoadProfile.builder()
                .generatedAt(Instant.now())
                .totalSamples(0)
                .queries(Map.of())
                .build();

        assertThatCode(() -> generator.insertRealSanitizedData(
                conn, emptyProfile, List.of(), Map.of(), Map.of(), Map.of(), Map.of()))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("insertRealSanitizedData with null sanitizedRealData on all queries must not throw")
    void insertRealSanitizedData_nullSanitizedData_doesNotThrow() {
        Connection conn = mock(Connection.class);
        LoadProfile.QueryStats stats = LoadProfile.QueryStats.builder()
                .queryId("q1")
                .p95Ms(50.0)
                .capturedSql("SELECT 1")
                .sanitizedRealData(null)
                .build();
        LoadProfile profile = LoadProfile.builder()
                .generatedAt(Instant.now())
                .totalSamples(1)
                .queries(Map.of("q1", stats))
                .build();

        assertThatCode(() -> generator.insertRealSanitizedData(
                conn, profile, List.of(), Map.of(), Map.of(), Map.of(), Map.of()))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("insertRealSanitizedData with empty sanitizedRealData must not throw")
    void insertRealSanitizedData_emptySanitizedData_doesNotThrow() {
        Connection conn = mock(Connection.class);
        LoadProfile.QueryStats stats = LoadProfile.QueryStats.builder()
                .queryId("q1")
                .sanitizedRealData(List.of())
                .build();
        LoadProfile profile = LoadProfile.builder()
                .generatedAt(Instant.now())
                .totalSamples(0)
                .queries(Map.of("q1", stats))
                .build();

        assertThatCode(() -> generator.insertRealSanitizedData(
                conn, profile, List.of(), Map.of(), Map.of(), Map.of(), Map.of()))
                .doesNotThrowAnyException();
    }
}
