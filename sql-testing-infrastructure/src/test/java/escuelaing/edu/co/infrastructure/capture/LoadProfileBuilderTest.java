package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.TransactionRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.Mockito.mock;

class LoadProfileBuilderTest {

    private LoadProfileBuilder builder;

    @BeforeEach
    void setUp() {
        builder = new LoadProfileBuilder(mock(MetricsBuffer.class));
    }

    @Test
    void p95_ofSequence1to20_is19() {
        LoadProfile.QueryStats stats = builder.buildFrom(latencySequence("q1", 1, 20))
                .getQueries().get("q1");
        // nearest-rank: ceil(0.95 × 20) − 1 = 18 → value = 19
        assertThat(stats.getP95Ms()).isEqualTo(19.0);
    }

    @Test
    void p99_ofSequence1to20_is20() {
        LoadProfile.QueryStats stats = builder.buildFrom(latencySequence("q1", 1, 20))
                .getQueries().get("q1");
        assertThat(stats.getP99Ms()).isEqualTo(20.0);
    }

    @Test
    void median_ofSequence1to20_is10() {
        LoadProfile.QueryStats stats = builder.buildFrom(latencySequence("q1", 1, 20))
                .getQueries().get("q1");
        // nearest-rank: ceil(0.50 × 20) − 1 = 9 → value = 10
        assertThat(stats.getMedianMs()).isEqualTo(10.0);
    }

    @Test
    void mean_ofSequence1to20_is10point5() {
        LoadProfile.QueryStats stats = builder.buildFrom(latencySequence("q1", 1, 20))
                .getQueries().get("q1");
        assertThat(stats.getMeanMs()).isCloseTo(10.5, within(0.001));
    }

    @Test
    void callsPerMinute_3callsIn2minutes_is1point5() {
        Instant base = Instant.now().minusSeconds(120);
        List<TransactionRecord> records = List.of(
                record("q1", 10L, base),
                record("q1", 20L, base.plusSeconds(60)),
                record("q1", 30L, base.plusSeconds(120))
        );
        assertThat(builder.buildFrom(records).getQueries().get("q1").getCallsPerMinute())
                .isCloseTo(1.5, within(0.1));
    }

    @Test
    void minAndMax_correspondToLowestAndHighestLatency() {
        LoadProfile.QueryStats stats = builder.buildFrom(latencySequence("q1", 5, 15))
                .getQueries().get("q1");
        assertThat(stats.getMinMs()).isEqualTo(5L);
        assertThat(stats.getMaxMs()).isEqualTo(15L);
    }

    @Test
    void sampleCount_reflectsTotalRecordsForQuery() {
        assertThat(builder.buildFrom(latencySequence("q1", 1, 10))
                .getQueries().get("q1").getSampleCount()).isEqualTo(10L);
    }

    @Test
    void capturedSql_isFirstNonBlankSqlFound() {
        Instant now = Instant.now();
        List<TransactionRecord> records = List.of(
                record("q1", 10L, now, "SELECT * FROM products WHERE category = ?"),
                record("q1", 20L, now.plusMillis(100), "SELECT * FROM products WHERE category = ?")
        );
        assertThat(builder.buildFrom(records).getQueries().get("q1").getCapturedSql())
                .isEqualTo("SELECT * FROM products WHERE category = ?");
    }

    @Test
    void emptyList_returnsProfileWithZeroQueriesAndSamples() {
        LoadProfile profile = builder.buildFrom(List.of());
        assertThat(profile.getQueries()).isEmpty();
        assertThat(profile.getTotalSamples()).isEqualTo(0L);
        assertThat(profile.getGeneratedAt()).isNotNull();
    }

    @Test
    void distinctQueryIds_producesSeparateEntriesInProfile() {
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
                .queryId(queryId).sql("SELECT 1")
                .latencyMs(latencyMs).timestamp(timestamp).build();
    }

    private TransactionRecord record(String queryId, long latencyMs, Instant timestamp, String sql) {
        return TransactionRecord.builder()
                .queryId(queryId).sql(sql)
                .latencyMs(latencyMs).timestamp(timestamp).build();
    }
}
