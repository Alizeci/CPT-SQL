package escuelaing.edu.co.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Result of a single benchmark execution (Phase 3).
 *
 * <p>Contains per-query metrics measured during the benchmark and a
 * {@code PASS/FAIL} verdict compared against {@code @Req} thresholds
 * and the stored baseline.</p>
 *
 * <p>Persisted as a versioned JSON artifact named
 * {@code benchmark-<profileName>-YYYYMMDD-HHmmss.json} (Phase 4).</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BenchmarkResult {

    /** Load profile name (e.g. "pre-test", "nightly"). */
    private String profileName;

    /** {@link TestProfile} name used (e.g. "light", "peak"). */
    private String testProfileName;

    /** Timestamp when the benchmark completed. */
    private Instant executedAt;

    /** SHA of the commit that triggered the run (null for manual executions). */
    private String commitSha;

    /** Total operations executed during the measurement window. */
    private long totalOperations;

    /** Per-queryId results. */
    private Map<String, QueryResult> queries;

    /** Overall verdict: PASS if all queries passed, FAIL if any failed. */
    private Verdict overallVerdict;

    /**
     * Observed TPS sampled every {@code metricsWindowSecs} seconds during
     * the measurement window. Enables throughput trend visualization over time.
     */
    private List<ThroughputSnapshot> throughputTimeSeries;

    /** Peak throughput reached during the benchmark, in TPS. */
    private double peakThroughputAchieved;

    // -------------------------------------------------------------------------

    public enum Verdict { PASS, FAIL }

    // -------------------------------------------------------------------------

    /**
     * Throughput snapshot for the time series.
     * Captured every {@code metricsWindowSecs} seconds (default 10 s).
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ThroughputSnapshot {

        /** Milliseconds elapsed since the start of the measurement window. */
        private long elapsedMs;

        /** Transactions per second observed in this window. */
        private double tps;

        /** TestProfile phase active at this moment. */
        private String phaseName;
    }

    /**
     * Per-query latency snapshot for the time series.
     * Enables tracking how p95 evolves across load phases.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LatencySnapshot {

        /** Milliseconds elapsed since the start of the measurement window. */
        private long elapsedMs;

        /** Median latency in this window, in milliseconds. */
        private double p50Ms;

        /** 95th percentile latency in this window, in milliseconds. */
        private double p95Ms;

        /** 99th percentile latency in this window, in milliseconds. */
        private double p99Ms;

        /** TestProfile phase active at this moment. */
        private String phaseName;
    }

    /**
     * Metrics and verdict for a single query within the benchmark.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class QueryResult {

        /** Query identifier — bridge with Phase 1. */
        private String queryId;

        /** Number of executions during the measurement window. */
        private long sampleCount;

        /** Mean latency, in milliseconds. */
        private double meanMs;

        /** Median latency (p50), in milliseconds. */
        private double medianMs;

        /** 95th percentile latency, in milliseconds. */
        private double p95Ms;

        /** 99th percentile latency, in milliseconds. */
        private double p99Ms;

        /** Minimum observed latency, in milliseconds. */
        private long minMs;

        /** Maximum observed latency, in milliseconds. */
        private long maxMs;

        /** Execution frequency during the benchmark, in calls per minute. */
        private double callsPerMinute;

        /**
         * Estimated planner cost captured via {@code EXPLAIN ANALYZE}.
         * Used to detect plan regressions between schema or statistics versions.
         */
        private double planCost;

        /**
         * Full execution plan text from the last {@code EXPLAIN ANALYZE} run for
         * this query. Complements {@code planCost} by showing structural changes
         * (e.g. index scan → seq scan) that a cost threshold alone cannot identify.
         */
        private String executionPlanText;

        /**
         * Percentage of operations that did not violate the SLA declared in {@code @Req}.
         * A value of 100.0 means all operations stayed within {@code maxResponseTimeMs}.
         */
        private double slaComplianceRate;

        /**
         * Percentage of the SLA threshold consumed by the measured p95.
         * Example: p95=86 ms with SLA=100 ms → slaRiskPct=86 %.
         * Values above 70 % indicate risk of violation under higher load.
         */
        private double slaRiskPct;

        /**
         * Latency evolution per time window during the measurement.
         * Enables detecting point-in-time spikes within the measurement window.
         */
        private List<LatencySnapshot> latencyTimeSeries;

        /** Individual verdict: PASS or FAIL. */
        private Verdict verdict;

        /**
         * Failure reason when {@code verdict == FAIL}, {@code null} when PASS.
         * Example: "p95=320ms exceeds maxResponseTimeMs=200ms".
         */
        private String failReason;
    }
}
