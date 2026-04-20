package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.TransactionRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JdbcWrapperLatencyTest {

    @Mock MetricsBuffer        metricsBuffer;
    @Mock SamplingFilter       samplingFilter;
    @Mock CaptureToggle        captureToggle;
    @Mock SanitizationStrategy sanitizationStrategy;
    @Mock Connection           mockConnection;
    @Mock PreparedStatement    mockPs;
    @Mock ResultSet            mockRs;

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

    @Test
    void executeQuery_recordsPositiveLatency() throws Exception {
        when(mockPs.executeQuery()).thenAnswer(inv -> {
            Thread.sleep(10);
            return mockRs;
        });

        Connection wrapped = wrapper.wrap(mockConnection);
        try (CaptureContext ctx = CaptureContext.begin(QUERY_ID)) {
            wrapped.prepareStatement(SQL).executeQuery();
        }

        ArgumentCaptor<TransactionRecord> captor = ArgumentCaptor.forClass(TransactionRecord.class);
        verify(metricsBuffer).record(captor.capture());
        assertThat(captor.getValue().getLatencyMs()).isGreaterThan(0L);
    }

    @Test
    void executeQuery_recordsCorrectQueryIdAndSql() throws Exception {
        when(mockPs.executeQuery()).thenReturn(mockRs);

        Connection wrapped = wrapper.wrap(mockConnection);
        try (CaptureContext ctx = CaptureContext.begin(QUERY_ID)) {
            wrapped.prepareStatement(SQL).executeQuery();
        }

        ArgumentCaptor<TransactionRecord> captor = ArgumentCaptor.forClass(TransactionRecord.class);
        verify(metricsBuffer).record(captor.capture());
        assertThat(captor.getValue().getQueryId()).isEqualTo(QUERY_ID);
        assertThat(captor.getValue().getSql()).isEqualTo(SQL);
        assertThat(captor.getValue().getTimestamp()).isNotNull();
    }

    @Test
    void executeQuery_withoutCaptureContext_doesNotRecord() throws Exception {
        when(mockPs.executeQuery()).thenReturn(mockRs);

        wrapper.wrap(mockConnection).prepareStatement(SQL).executeQuery();

        verifyNoInteractions(metricsBuffer);
    }
}
