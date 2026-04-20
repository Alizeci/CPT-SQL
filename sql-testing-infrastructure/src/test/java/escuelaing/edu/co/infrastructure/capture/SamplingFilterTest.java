package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.QueryEntry;
import escuelaing.edu.co.infrastructure.analysis.QueryRegistryLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SamplingFilterTest {

    private QueryRegistryLoader registry;
    private SamplingFilter filter;

    @BeforeEach
    void setUp() {
        registry = mock(QueryRegistryLoader.class);
        filter   = new SamplingFilter(registry);
    }

    @Test
    void highPriority_alwaysRecords() {
        when(registry.get("q1")).thenReturn(entry("HIGH", 300, true));
        for (int i = 0; i < 50; i++) {
            assertThat(filter.shouldRecord("q1", 10L)).isTrue();
        }
    }

    @Test
    void latencyExceedsMaxResponseTimeMs_alwaysRecords() {
        when(registry.get("q1")).thenReturn(entry("MEDIUM", 100, true));
        assertThat(filter.shouldRecord("q1", 500L)).isTrue();
    }

    @Test
    void nullQueryId_neverRecords() {
        assertThat(filter.shouldRecord(null, 10L)).isFalse();
    }

    @Test
    void unknownQueryId_appliesProbabilisticSampling() {
        when(registry.get("unknown")).thenReturn(null);
        // Over 1000 calls at 10% rate, expect between 50 and 200 records (very wide bounds)
        long recorded = countRecorded("unknown", 10L, 1000);
        assertThat(recorded).isBetween(50L, 200L);
    }

    @Test
    void mediumPriority_belowSla_appliesProbabilisticSampling() {
        when(registry.get("q1")).thenReturn(entry("MEDIUM", 300, true));
        long recorded = countRecorded("q1", 10L, 1000);
        assertThat(recorded).isBetween(50L, 200L);
    }

    private long countRecorded(String queryId, long latencyMs, int iterations) {
        long count = 0;
        for (int i = 0; i < iterations; i++) {
            if (filter.shouldRecord(queryId, latencyMs)) count++;
        }
        return count;
    }

    private QueryEntry entry(String priority, long maxResponseTimeMs, boolean hasReq) {
        return QueryEntry.builder()
                .queryId("q1")
                .priority(priority)
                .maxResponseTimeMs(maxResponseTimeMs)
                .hasReq(hasReq)
                .build();
    }
}
