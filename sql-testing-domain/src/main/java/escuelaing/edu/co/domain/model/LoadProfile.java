package escuelaing.edu.co.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Load profile ({@code load-profile.json}): per-{@code queryId} latency and frequency
 * statistics recorded in the capture environment over an observation window, plus the
 * differentially private release of the data. Produced by the capture environment and
 * consumed to generate and load the mirror database.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LoadProfile {

    private Instant generatedAt;
    private long totalSamples;
    private Map<String, QueryStats> queries;

    /**
     * Differentially private release of the business columns and foreign-key concentration
     * curves. When present, the mirror database is populated only from this release;
     * when {@code null}, generation falls back to unprotected statistics computed from
     * {@code sanitizedRealData}.
     */
    private DpRelease dpRelease;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class QueryStats {

        private String queryId;
        private long   sampleCount;
        // Latency statistics in milliseconds; percentiles are nearest-rank.
        private double meanMs;
        private double medianMs;
        private double p95Ms;
        private double p99Ms;
        private long   minMs;
        private long   maxMs;

        /** Call rate over the observation window. */
        private double callsPerMinute;

        /** First non-blank SQL text seen for this query. */
        private String capturedSql;

        /** Mean rows affected over executions that affected rows; {@code 0} for reads. */
        private double avgRowCount;

        /**
         * Raw sanitized rows; {@code null} unless explicitly retained for debugging. Not
         * protected by the differentially private release.
         */
        private List<Map<String, Object>> sanitizedRealData;
    }
}
