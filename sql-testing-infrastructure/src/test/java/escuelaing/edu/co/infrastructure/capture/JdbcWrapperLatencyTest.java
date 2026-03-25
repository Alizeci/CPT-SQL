package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.TransactionRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifica que {@link JdbcWrapper} mide latencias reales (> 0 ms).
 *
 * <p>El bug original (T1-A) capturaba {@code startNano} DESPUÉS de ejecutar
 * la query, produciendo siempre 0 ms. Estos tests confirman que el fix —
 * capturar {@code startNano} ANTES de la ejecución — funciona correctamente.</p>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JdbcWrapperLatencyTest {

    @Mock MetricsBuffer         metricsBuffer;
    @Mock SamplingFilter        samplingFilter;
    @Mock CaptureToggle         captureToggle;
    @Mock SanitizationStrategy  sanitizationStrategy;
    @Mock Connection            mockConnection;
    @Mock PreparedStatement     mockPs;
    @Mock ResultSet             mockRs;

    private JdbcWrapper wrapper;

    private static final String QUERY_ID = "testQuery";
    private static final String SQL      = "SELECT 1";

    @BeforeEach
    void setUp() throws Exception {
        wrapper = new JdbcWrapper(samplingFilter, metricsBuffer, captureToggle, sanitizationStrategy);

        when(captureToggle.isEnabled()).thenReturn(true);
        when(samplingFilter.shouldRecord(anyString(), anyLong())).thenReturn(true);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPs);
    }

    // -------------------------------------------------------------------------

    @Test
    @DisplayName("executeQuery con 10 ms de latencia real debe registrar latencyMs > 0")
    void executeQuery_recordsPositiveLatency() throws Exception {
        when(mockPs.executeQuery()).thenAnswer(inv -> {
            Thread.sleep(10); // simula latencia real de 10 ms
            return mockRs;
        });

        Connection wrapped = wrapper.wrap(mockConnection);
        try (CaptureContext ctx = CaptureContext.begin(QUERY_ID)) {
            PreparedStatement ps = wrapped.prepareStatement(SQL);
            ps.executeQuery();
        }

        ArgumentCaptor<TransactionRecord> captor = ArgumentCaptor.forClass(TransactionRecord.class);
        verify(metricsBuffer).record(captor.capture());

        TransactionRecord recorded = captor.getValue();
        assertThat(recorded.getLatencyMs())
                .as("latencyMs debe ser > 0 — el timing fix garantiza que startNano " +
                    "se captura antes de la ejecución")
                .isGreaterThan(0L);
    }

    @Test
    @DisplayName("El TransactionRecord debe tener el queryId y SQL correctos")
    void executeQuery_recordsCorrectQueryIdAndSql() throws Exception {
        when(mockPs.executeQuery()).thenReturn(mockRs);

        Connection wrapped = wrapper.wrap(mockConnection);
        try (CaptureContext ctx = CaptureContext.begin(QUERY_ID)) {
            wrapped.prepareStatement(SQL).executeQuery();
        }

        ArgumentCaptor<TransactionRecord> captor = ArgumentCaptor.forClass(TransactionRecord.class);
        verify(metricsBuffer).record(captor.capture());

        TransactionRecord recorded = captor.getValue();
        assertThat(recorded.getQueryId()).isEqualTo(QUERY_ID);
        assertThat(recorded.getSql()).isEqualTo(SQL);
        assertThat(recorded.getTimestamp()).isNotNull();
    }

    @Test
    @DisplayName("Sin CaptureContext activo, el wrapper no debe registrar nada")
    void executeQuery_withoutContext_doesNotRecord() throws Exception {
        when(mockPs.executeQuery()).thenReturn(mockRs);

        Connection wrapped = wrapper.wrap(mockConnection);
        // No CaptureContext.begin() — queryId es null
        wrapped.prepareStatement(SQL).executeQuery();

        // samplingFilter.shouldRecord(null, ...) → false (ver SamplingFilter)
        // por lo tanto metricsBuffer.record() nunca debe llamarse
        org.mockito.Mockito.verifyNoInteractions(metricsBuffer);
    }
}
