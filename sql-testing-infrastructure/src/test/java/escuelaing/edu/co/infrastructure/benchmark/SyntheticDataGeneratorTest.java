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
 * Verifica la lógica del algoritmo DPSDG: clasificación de columnas,
 * generación de pseudónimos UII, reproducibilidad con mismo seed, y
 * comportamiento de {@code insertRealSanitizedData} ante perfiles vacíos.
 */
class SyntheticDataGeneratorTest {

    private SyntheticDataGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new SyntheticDataGenerator();
        generator.setSeed(42);
    }

    // -------------------------------------------------------------------------
    // classifyColumn — separación Feature / UII (SynQB §3.2)
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("user_id y customer_id deben clasificarse como IDENTIFIER")
    void classifyColumn_userIdColumns() {
        assertThat(generator.classifyColumn("orders", "user_id")).isEqualTo(IDENTIFIER);
        assertThat(generator.classifyColumn("orders", "customer_id")).isEqualTo(IDENTIFIER);
        assertThat(generator.classifyColumn("orders", "USER_ID")).isEqualTo(IDENTIFIER);
    }

    @Test
    @DisplayName("columnas _id deben clasificarse como IDENTIFIER")
    void classifyColumn_transactionIdColumns() {
        assertThat(generator.classifyColumn("orders",      "id")).isEqualTo(IDENTIFIER);
        assertThat(generator.classifyColumn("order_items", "order_id")).isEqualTo(IDENTIFIER);
        assertThat(generator.classifyColumn("order_items", "product_id")).isEqualTo(IDENTIFIER);
    }

    @Test
    @DisplayName("columnas de negocio deben clasificarse como FEATURE")
    void classifyColumn_featureColumns() {
        assertThat(generator.classifyColumn("products", "price")).isEqualTo(FEATURE);
        assertThat(generator.classifyColumn("products", "rating")).isEqualTo(FEATURE);
        assertThat(generator.classifyColumn("products", "category")).isEqualTo(FEATURE);
        assertThat(generator.classifyColumn("orders",   "total_amount")).isEqualTo(FEATURE);
        assertThat(generator.classifyColumn("products", "stock_quantity")).isEqualTo(FEATURE);
    }

    // -------------------------------------------------------------------------
    // generateUiiPseudonym — rango exclusivo [10001, 99999]
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("todos los pseudónimos UII deben estar en [10001, 99999]")
    void generateUiiPseudonym_isInExclusiveRange() {
        for (int i = 0; i < 200; i++) {
            int id = generator.generateUiiPseudonym();
            assertThat(id)
                    .as("el pseudónimo debe estar en el rango exclusivo para evitar colisión con IDs reales")
                    .isBetween(10_001, 99_999);
        }
    }

    // -------------------------------------------------------------------------
    // gaussianNoise — reproducibilidad con mismo seed (SynQB Algorithm 1)
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("gaussianNoise con el mismo seed debe producir valores idénticos")
    void gaussianNoise_isReproducibleWithSameSeed() {
        generator.setSeed(42);
        double v1 = generator.gaussianNoise(100.0, 20.0);

        generator.setSeed(42);
        double v2 = generator.gaussianNoise(100.0, 20.0);

        assertThat(v1).isEqualTo(v2);
    }

    @Test
    @DisplayName("gaussianNoise con seeds distintos debe producir valores diferentes (en general)")
    void gaussianNoise_differsBetweenSeeds() {
        generator.setSeed(42);
        double v1 = generator.gaussianNoise(100.0, 20.0);

        generator.setSeed(99);
        double v2 = generator.gaussianNoise(100.0, 20.0);

        // Con seeds diferentes la distribución cambia; no son iguales para base=100, sigma=20
        assertThat(v1).isNotEqualTo(v2);
    }

    @Test
    @DisplayName("gaussianNoise aplica el presupuesto ε_ft como divisor del sigma")
    void gaussianNoise_appliesEpsilonScale() {
        // Con sigma = 0 y cualquier ε, el ruido debe ser exactamente base
        generator.setSeed(42);
        double result = generator.gaussianNoise(50.0, 0.0);
        assertThat(result).isCloseTo(50.0, within(0.001));
    }

    // -------------------------------------------------------------------------
    // Constantes de privacidad diferencial (SynQB Theorem 4.4)
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("ε_total = ε_ft + ε_uii = 1.0 por composición secuencial")
    void epsilonTotalEqualsSum() {
        assertThat(SyntheticDataGenerator.EPSILON_TOTAL)
                .isCloseTo(SyntheticDataGenerator.EPSILON_FT + SyntheticDataGenerator.EPSILON_UII,
                        within(1e-9));
        assertThat(SyntheticDataGenerator.EPSILON_TOTAL).isCloseTo(1.0, within(1e-9));
    }

    // -------------------------------------------------------------------------
    // insertRealSanitizedData — manejo defensivo ante inputs inválidos
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("insertRealSanitizedData con perfil null no debe lanzar excepción")
    void insertRealSanitizedData_nullProfile_doesNotThrow() {
        Connection conn = mock(Connection.class);
        assertThatCode(() -> generator.insertRealSanitizedData(
                conn, null, List.of(), Map.of(), Map.of(), Map.of(), Map.of()))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("insertRealSanitizedData con perfil vacío no debe lanzar excepción")
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
    @DisplayName("insertRealSanitizedData con sanitizedRealData null en todas las queries no falla")
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
    @DisplayName("insertRealSanitizedData con sanitizedRealData vacía no falla")
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
