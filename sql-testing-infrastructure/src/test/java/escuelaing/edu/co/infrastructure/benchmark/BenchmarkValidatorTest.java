package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.TransactionRecord;
import escuelaing.edu.co.domain.model.validation.CardinalityFidelity;
import escuelaing.edu.co.domain.model.validation.LatencyFidelity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Validates the three fidelity dimensions implemented in {@link FidelityValidator}:
 * latency, cardinality, and correct skipping of SELECT queries.
 *
 * <p>Infrastructure dependencies ({@link MirrorDatabaseProvisioner},
 * {@link SyntheticDataGenerator}, {@link QueryExecutor}) are mocked to isolate
 * the statistical calculation logic that determines PASS / FAIL.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BenchmarkValidatorTest {

    @Mock MirrorDatabaseProvisioner provisioner;
    @Mock SyntheticDataGenerator    dataGenerator;
    @Mock QueryExecutor             queryExecutor;
    @Mock Connection                conn;
    @Mock PreparedStatement         writePs;

    private FidelityValidator fidelityValidator;

    @BeforeEach
    void setUp() {
        fidelityValidator = new FidelityValidator(provisioner, dataGenerator, queryExecutor);
    }

    // -------------------------------------------------------------------------
    // validateLatencyFidelity — PASS
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("latencyFidelity PASS when synthetic p95 ≈ real p95 (error < 10 %)")
    void validateLatencyFidelity_pass_whenSynMatchesReal() {
        // p95 real = 50 ms; all synthetic runs return 50 ms → error = 0 %
        LoadProfile profile = profileWithLatency("q1", 50.0, "SELECT 1");
        when(queryExecutor.execute(any(Connection.class), eq("q1"), any(), anyString()))
                .thenReturn(record("q1", 50L));

        Map<String, LatencyFidelity> result = fidelityValidator.validateLatencyFidelity(conn, profile);

        assertThat(result).containsKey("q1");
        assertThat(result.get("q1").isPass()).isTrue();
        assertThat(result.get("q1").getErrorPct()).isCloseTo(0.0, within(0.1));
        assertThat(result.get("q1").getP95RealMs()).isEqualTo(50L);
    }

    @Test
    @DisplayName("latencyFidelity PASS when error < 10 % (synthetic slightly different)")
    void validateLatencyFidelity_pass_whenErrorBelowThreshold() {
        // p95 real = 100 ms; synthetic = 105 ms → error = 5 % < 10 % → PASS
        LoadProfile profile = profileWithLatency("q1", 100.0, "SELECT 1");
        when(queryExecutor.execute(any(Connection.class), eq("q1"), any(), anyString()))
                .thenReturn(record("q1", 105L));

        Map<String, LatencyFidelity> result = fidelityValidator.validateLatencyFidelity(conn, profile);

        assertThat(result.get("q1").isPass()).isTrue();
        assertThat(result.get("q1").getErrorPct()).isCloseTo(5.0, within(0.5));
    }

    // -------------------------------------------------------------------------
    // validateLatencyFidelity — FAIL
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("latencyFidelity FAIL when synthetic p95 differs > 10 % from real")
    void validateLatencyFidelity_fail_whenSynDiffersFromReal() {
        // p95 real = 10 ms; synthetic = 100 ms → error = 900 % → FAIL
        LoadProfile profile = profileWithLatency("q1", 10.0, "SELECT 1");
        when(queryExecutor.execute(any(Connection.class), eq("q1"), any(), anyString()))
                .thenReturn(record("q1", 100L));

        Map<String, LatencyFidelity> result = fidelityValidator.validateLatencyFidelity(conn, profile);

        assertThat(result.get("q1").isPass()).isFalse();
        assertThat(result.get("q1").getErrorPct()).isGreaterThan(10.0);
    }

    @Test
    @DisplayName("latencyFidelity skips queries with no capturedSql")
    void validateLatencyFidelity_skipsQueriesWithoutSql() {
        LoadProfile profile = profileWithLatency("q1", 50.0, null); // capturedSql = null

        Map<String, LatencyFidelity> result = fidelityValidator.validateLatencyFidelity(conn, profile);

        assertThat(result).doesNotContainKey("q1");
        verifyNoInteractions(queryExecutor);
    }

    // -------------------------------------------------------------------------
    // validateCardinalityFidelity — SELECT skipping
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("cardinalityFidelity skips SELECT queries (avgRowCount == 0)")
    void validateCardinalityFidelity_skips_selectQueries() {
        // avgRowCount = 0 → query is SELECT → must be skipped
        LoadProfile profile = profileWithLatency("selectQ", 50.0, "SELECT * FROM products");
        // profileWithLatency stats have avgRowCount = 0 by default

        Map<String, CardinalityFidelity> result = fidelityValidator.validateCardinalityFidelity(conn, profile);

        assertThat(result).doesNotContainKey("selectQ");
        verifyNoInteractions(queryExecutor);
    }

    @Test
    @DisplayName("cardinalityFidelity skips queries with no capturedSql")
    void validateCardinalityFidelity_skipsQueriesWithoutSql() {
        LoadProfile profile = profileWithLatency("q1", 50.0, null);

        Map<String, CardinalityFidelity> result = fidelityValidator.validateCardinalityFidelity(conn, profile);

        assertThat(result).doesNotContainKey("q1");
    }

    @Test
    @DisplayName("cardinalityFidelity evaluates write queries (avgRowCount > 0)")
    void validateCardinalityFidelity_evaluatesWriteQueries() throws Exception {
        // countWriteAffectedRows uses conn.prepareStatement — must return a valid mock
        when(conn.prepareStatement(anyString())).thenReturn(writePs);
        when(writePs.executeUpdate()).thenReturn(3);

        // avgRowCount = 3 → DML query → must be evaluated
        LoadProfile.QueryStats writeStats = LoadProfile.QueryStats.builder()
                .queryId("updateQ")
                .p95Ms(30.0)
                .capturedSql("UPDATE products SET stock_quantity = ? WHERE id = ?")
                .avgRowCount(3.0)
                .build();
        LoadProfile profile = LoadProfile.builder()
                .generatedAt(Instant.now())
                .totalSamples(10)
                .queries(Map.of("updateQ", writeStats))
                .build();

        // countWriteAffectedRows is private; result depends on the driver.
        // Without a real DB, the query fails silently → rowsSynthetic = 0.
        // Only verify that the entry appears in the result map.
        Map<String, CardinalityFidelity> result = fidelityValidator.validateCardinalityFidelity(conn, profile);

        assertThat(result).containsKey("updateQ");
        assertThat(result.get("updateQ").getQueryId()).isEqualTo("updateQ");
        assertThat(result.get("updateQ").getRowsReal()).isEqualTo(3L);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Builds a {@link LoadProfile} with a single query whose real p95 and SQL are known.
     * {@code avgRowCount} defaults to 0 (SELECT query).
     */
    private LoadProfile profileWithLatency(String queryId, double p95RealMs, String capturedSql) {
        LoadProfile.QueryStats stats = LoadProfile.QueryStats.builder()
                .queryId(queryId)
                .p95Ms(p95RealMs)
                .capturedSql(capturedSql)
                .avgRowCount(0.0)
                .build();
        return LoadProfile.builder()
                .generatedAt(Instant.now())
                .totalSamples(10)
                .queries(Map.of(queryId, stats))
                .build();
    }

    private TransactionRecord record(String queryId, long latencyMs) {
        return TransactionRecord.builder()
                .queryId(queryId)
                .sql("SELECT 1")
                .latencyMs(latencyMs)
                .timestamp(Instant.now())
                .build();
    }
}
