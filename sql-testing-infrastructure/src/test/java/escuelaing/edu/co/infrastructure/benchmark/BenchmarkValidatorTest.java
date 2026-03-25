package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.TransactionRecord;
import escuelaing.edu.co.domain.model.validation.CardinalityFidelity;
import escuelaing.edu.co.domain.model.validation.LatencyFidelity;
import escuelaing.edu.co.infrastructure.analysis.QueryRegistryLoader;
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
 * Verifica las tres dimensiones de validación de fidelidad implementadas en
 * {@link BenchmarkRunner}: latencia, cardinalidad y omisión correcta de SELECTs.
 *
 * <p>Se mockean las dependencias de infraestructura ({@link MirrorDatabaseProvisioner},
 * {@link SyntheticDataGenerator}, {@link QueryExecutor}) para aislar la lógica
 * de cálculo estadístico que determina PASS / FAIL.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BenchmarkValidatorTest {

    @Mock MirrorDatabaseProvisioner provisioner;
    @Mock SyntheticDataGenerator    dataGenerator;
    @Mock QueryExecutor             queryExecutor;
    @Mock QueryRegistryLoader       queryRegistry;
    @Mock Connection                conn;
    @Mock PreparedStatement         writePs;

    private BenchmarkRunner runner;

    @BeforeEach
    void setUp() {
        runner = new BenchmarkRunner(provisioner, dataGenerator, queryExecutor, queryRegistry);
    }

    // -------------------------------------------------------------------------
    // validateLatencyFidelity — PASS
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("latencyFidelity PASS cuando p95 sintético ≈ p95 real (error < 10 %)")
    void validateLatencyFidelity_pass_whenSynMatchesReal() {
        // p95 real = 50 ms; todos los 5 runs sintéticos devuelven 50 ms → error = 0 %
        LoadProfile profile = profileWithLatency("q1", 50.0, "SELECT 1");
        when(queryExecutor.execute(any(Connection.class), eq("q1"), any(), anyString()))
                .thenReturn(record("q1", 50L));

        Map<String, LatencyFidelity> result = runner.validateLatencyFidelity(conn, profile);

        assertThat(result).containsKey("q1");
        assertThat(result.get("q1").isPass()).isTrue();
        assertThat(result.get("q1").getErrorPct()).isCloseTo(0.0, within(0.1));
        assertThat(result.get("q1").getP95RealMs()).isEqualTo(50L);
    }

    @Test
    @DisplayName("latencyFidelity PASS cuando error < 10 % (sintético levemente distinto)")
    void validateLatencyFidelity_pass_whenErrorBelowThreshold() {
        // p95 real = 100 ms; sintético = 105 ms → error = 5 % < 10 % → PASS
        LoadProfile profile = profileWithLatency("q1", 100.0, "SELECT 1");
        when(queryExecutor.execute(any(Connection.class), eq("q1"), any(), anyString()))
                .thenReturn(record("q1", 105L));

        Map<String, LatencyFidelity> result = runner.validateLatencyFidelity(conn, profile);

        assertThat(result.get("q1").isPass()).isTrue();
        assertThat(result.get("q1").getErrorPct()).isCloseTo(5.0, within(0.5));
    }

    // -------------------------------------------------------------------------
    // validateLatencyFidelity — FAIL
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("latencyFidelity FAIL cuando p95 sintético difiere > 10 % del real")
    void validateLatencyFidelity_fail_whenSynDiffersFromReal() {
        // p95 real = 10 ms; sintético = 100 ms → error = 900 % → FAIL
        LoadProfile profile = profileWithLatency("q1", 10.0, "SELECT 1");
        when(queryExecutor.execute(any(Connection.class), eq("q1"), any(), anyString()))
                .thenReturn(record("q1", 100L));

        Map<String, LatencyFidelity> result = runner.validateLatencyFidelity(conn, profile);

        assertThat(result.get("q1").isPass()).isFalse();
        assertThat(result.get("q1").getErrorPct()).isGreaterThan(10.0);
    }

    @Test
    @DisplayName("latencyFidelity omite queries sin capturedSql")
    void validateLatencyFidelity_skipsQueriesWithoutSql() {
        LoadProfile profile = profileWithLatency("q1", 50.0, null); // capturedSql = null

        Map<String, LatencyFidelity> result = runner.validateLatencyFidelity(conn, profile);

        assertThat(result).doesNotContainKey("q1");
        verifyNoInteractions(queryExecutor);
    }

    // -------------------------------------------------------------------------
    // validateCardinalityFidelity — omisión de SELECTs
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("cardinalityFidelity omite queries SELECT (avgRowCount == 0)")
    void validateCardinalityFidelity_skips_selectQueries() {
        // avgRowCount = 0 → query es SELECT → debe omitirse
        LoadProfile profile = profileWithLatency("selectQ", 50.0, "SELECT * FROM products");
        // el stats de profileWithLatency tiene avgRowCount = 0 por defecto

        Map<String, CardinalityFidelity> result = runner.validateCardinalityFidelity(conn, profile);

        assertThat(result).doesNotContainKey("selectQ");
        verifyNoInteractions(queryExecutor);
    }

    @Test
    @DisplayName("cardinalityFidelity omite queries sin capturedSql")
    void validateCardinalityFidelity_skipsQueriesWithoutSql() {
        LoadProfile profile = profileWithLatency("q1", 50.0, null);

        Map<String, CardinalityFidelity> result = runner.validateCardinalityFidelity(conn, profile);

        assertThat(result).doesNotContainKey("q1");
    }

    @Test
    @DisplayName("cardinalityFidelity evalúa queries de escritura (avgRowCount > 0)")
    void validateCardinalityFidelity_evaluatesWriteQueries() throws Exception {
        // countWriteAffectedRows usa conn.prepareStatement — debe retornar un mock válido
        when(conn.prepareStatement(anyString())).thenReturn(writePs);
        when(writePs.executeUpdate()).thenReturn(3);

        // avgRowCount = 3 → es DML → debe evaluarse
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

        // countWriteAffectedRows es privado; el resultado depende del driver.
        // En test sin BD real, la query falla silenciosamente → rowsSynthetic = 0.
        // Solo verificamos que la entrada aparece en el mapa de resultados.
        Map<String, CardinalityFidelity> result = runner.validateCardinalityFidelity(conn, profile);

        assertThat(result).containsKey("updateQ");
        assertThat(result.get("updateQ").getQueryId()).isEqualTo("updateQ");
        assertThat(result.get("updateQ").getRowsReal()).isEqualTo(3L);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Construye un {@link LoadProfile} con una sola query cuya p95 real y SQL son conocidos.
     * {@code avgRowCount} queda en 0 (query SELECT) por defecto.
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
