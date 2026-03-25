package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.TransactionRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.Mockito.mock;

/**
 * Verifica el cálculo estadístico de {@link LoadProfileBuilder}:
 * percentiles (p50, p95, p99), media, callsPerMinute y manejo de lista vacía.
 */
class LoadProfileBuilderTest {

    private LoadProfileBuilder builder;

    @BeforeEach
    void setUp() {
        // MetricsBuffer no es relevante para buildFrom() — se mockea para aislar la lógica
        builder = new LoadProfileBuilder(mock(MetricsBuffer.class));
    }

    // -------------------------------------------------------------------------
    // Percentiles
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("p95 de [1..20] con nearest-rank debe ser 19")
    void buildFrom_computesP95Correctly() {
        List<TransactionRecord> records = latencySequence("q1", 1, 20);

        LoadProfile.QueryStats stats = builder.buildFrom(records).getQueries().get("q1");

        // nearest-rank: índice = ceil(0.95 × 20) − 1 = 19 − 1 = 18 → valor = 19
        assertThat(stats.getP95Ms()).isEqualTo(19.0);
    }

    @Test
    @DisplayName("p99 de [1..20] con nearest-rank debe ser 20")
    void buildFrom_computesP99Correctly() {
        List<TransactionRecord> records = latencySequence("q1", 1, 20);

        LoadProfile.QueryStats stats = builder.buildFrom(records).getQueries().get("q1");

        // nearest-rank: índice = ceil(0.99 × 20) − 1 = 20 − 1 = 19 → valor = 20
        assertThat(stats.getP99Ms()).isEqualTo(20.0);
    }

    @Test
    @DisplayName("mediana (p50) de [1..20] con nearest-rank debe ser 10")
    void buildFrom_computesMedianCorrectly() {
        List<TransactionRecord> records = latencySequence("q1", 1, 20);

        LoadProfile.QueryStats stats = builder.buildFrom(records).getQueries().get("q1");

        // nearest-rank: ceil(0.50 × 20) − 1 = 10 − 1 = 9 → valor = 10
        assertThat(stats.getMedianMs()).isEqualTo(10.0);
    }

    @Test
    @DisplayName("media aritmética de [1..20] debe ser 10.5")
    void buildFrom_computesMeanCorrectly() {
        List<TransactionRecord> records = latencySequence("q1", 1, 20);

        LoadProfile.QueryStats stats = builder.buildFrom(records).getQueries().get("q1");

        assertThat(stats.getMeanMs()).isCloseTo(10.5, within(0.001));
    }

    // -------------------------------------------------------------------------
    // callsPerMinute
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("3 llamadas en ventana de 2 minutos → callsPerMinute ≈ 1.5")
    void buildFrom_computesCallsPerMinute() {
        Instant base = Instant.now().minusSeconds(120);
        List<TransactionRecord> records = List.of(
                record("q1", 10L, base),
                record("q1", 20L, base.plusSeconds(60)),
                record("q1", 30L, base.plusSeconds(120))
        );

        LoadProfile.QueryStats stats = builder.buildFrom(records).getQueries().get("q1");

        // 3 samples / 2 min = 1.5 cpm
        assertThat(stats.getCallsPerMinute()).isCloseTo(1.5, within(0.1));
    }

    // -------------------------------------------------------------------------
    // min / max
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("minMs y maxMs deben corresponder al menor y mayor valor")
    void buildFrom_computesMinAndMax() {
        List<TransactionRecord> records = latencySequence("q1", 5, 15);

        LoadProfile.QueryStats stats = builder.buildFrom(records).getQueries().get("q1");

        assertThat(stats.getMinMs()).isEqualTo(5L);
        assertThat(stats.getMaxMs()).isEqualTo(15L);
    }

    // -------------------------------------------------------------------------
    // sampleCount y capturedSql
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("sampleCount debe reflejar el total de registros de esa query")
    void buildFrom_computesSampleCount() {
        List<TransactionRecord> records = latencySequence("q1", 1, 10);

        LoadProfile.QueryStats stats = builder.buildFrom(records).getQueries().get("q1");

        assertThat(stats.getSampleCount()).isEqualTo(10L);
    }

    @Test
    @DisplayName("capturedSql debe ser el primer SQL no nulo encontrado")
    void buildFrom_capturesSql() {
        Instant now = Instant.now();
        List<TransactionRecord> records = List.of(
                record("q1", 10L, now, "SELECT * FROM products WHERE category = ?"),
                record("q1", 20L, now.plusMillis(100), "SELECT * FROM products WHERE category = ?")
        );

        LoadProfile.QueryStats stats = builder.buildFrom(records).getQueries().get("q1");

        assertThat(stats.getCapturedSql()).isEqualTo("SELECT * FROM products WHERE category = ?");
    }

    // -------------------------------------------------------------------------
    // Lista vacía
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("lista vacía debe producir un perfil con 0 queries y 0 muestras")
    void buildFrom_emptyList_returnsEmptyProfile() {
        LoadProfile profile = builder.buildFrom(List.of());

        assertThat(profile.getQueries()).isEmpty();
        assertThat(profile.getTotalSamples()).isEqualTo(0L);
        assertThat(profile.getGeneratedAt()).isNotNull();
    }

    // -------------------------------------------------------------------------
    // Múltiples queries
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("registros de dos queryIds distintos producen dos entradas en el mapa")
    void buildFrom_groupsByQueryId() {
        Instant now = Instant.now();
        List<TransactionRecord> records = List.of(
                record("searchProducts", 30L, now),
                record("createOrder",    50L, now.plusMillis(10)),
                record("searchProducts", 40L, now.plusMillis(20))
        );

        LoadProfile profile = builder.buildFrom(records);

        assertThat(profile.getQueries()).containsKeys("searchProducts", "createOrder");
        assertThat(profile.getQueries().get("searchProducts").getSampleCount()).isEqualTo(2L);
        assertThat(profile.getQueries().get("createOrder").getSampleCount()).isEqualTo(1L);
        assertThat(profile.getTotalSamples()).isEqualTo(3L);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Genera {@code (end - start + 1)} registros con latencias start..end para una sola query. */
    private List<TransactionRecord> latencySequence(String queryId, int start, int end) {
        Instant base = Instant.now().minusSeconds(60);
        List<TransactionRecord> records = new ArrayList<>();
        for (int i = start; i <= end; i++) {
            records.add(record(queryId, (long) i, base.plusMillis((long) i * 500)));
        }
        return records;
    }

    private TransactionRecord record(String queryId, long latencyMs, Instant timestamp) {
        return TransactionRecord.builder()
                .queryId(queryId)
                .sql("SELECT 1")
                .latencyMs(latencyMs)
                .timestamp(timestamp)
                .build();
    }

    private TransactionRecord record(String queryId, long latencyMs, Instant timestamp, String sql) {
        return TransactionRecord.builder()
                .queryId(queryId)
                .sql(sql)
                .latencyMs(latencyMs)
                .timestamp(timestamp)
                .build();
    }
}
