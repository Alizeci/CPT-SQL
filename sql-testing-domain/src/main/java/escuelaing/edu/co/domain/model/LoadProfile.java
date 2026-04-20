package escuelaing.edu.co.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Production load profile: statistical summary of queries captured during
 * an observation window.
 *
 * <p>Output of phase 2 and input to phase 3. Contains per-{@code queryId}
 * aggregated metrics needed to synthesise load on the mirror database.</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LoadProfile {

    private Instant generatedAt;
    private long totalSamples;
    private Map<String, QueryStats> queries;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class QueryStats {

        private String queryId;
        private long   sampleCount;
        private double meanMs;
        private double medianMs;
        private double p95Ms;
        private double p99Ms;
        private long   minMs;
        private long   maxMs;

        private double callsPerMinute;

        private String capturedSql;

        private double avgRowCount;

        private List<Map<String, Object>> sanitizedRealData;
    }
}
