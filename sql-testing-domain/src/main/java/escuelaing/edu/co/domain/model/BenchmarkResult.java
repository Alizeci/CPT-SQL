package escuelaing.edu.co.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Result of a single benchmark run against the mirror database.
 *
 * <p>Contains per-query metrics from the measurement phases and a {@code PASS/FAIL}
 * verdict against the {@code @Req} contracts only; baseline comparison is done separately
 * by the degradation detector.</p>
 *
 * <p>Persisted as {@code build/results/benchmark-<profileName>-yyyyMMdd-HHmmss.json}
 * (UTC timestamp).</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BenchmarkResult {

    /** Run label from {@code loadtest.benchmark.profileName} (default "nightly"). */
    private String profileName;

    /** {@link TestProfile} name used (e.g. "light", "peak"). */
    private String testProfileName;

    /** Timestamp when the benchmark completed. */
    private Instant executedAt;

    /** SHA of the commit that triggered the run (null for manual executions). */
    private String commitSha;

    /** Total operations executed during the measurement phases. */
    private long totalOperations;

    /** Per-queryId results. */
    private Map<String, QueryResult> queries;

    /** PASS if every query verdict is PASS, FAIL otherwise. */
    private Verdict overallVerdict;

    /**
     * Observed throughput sampled every {@code metricsWindowSecs} seconds during the
     * measurement phases.
     */
    private List<ThroughputSnapshot> throughputTimeSeries;

    /** Highest windowed throughput reached during measurement, in operations per second. */
    private double peakThroughputAchieved;

    /** Synthetic rows per table of the mirror database used in this run. */
    private long rowsPerTable;

    /**
     * Fingerprint (first 8 bytes of SHA-256, hex) of the load profile inputs that determine
     * the mirror data: the differentially private release, or the sanitized rows for
     * profiles without one. A run is only compared against a baseline with the same value.
     */
    private String loadProfileFingerprint;

    public enum Verdict { PASS, FAIL }

    /**
     * Throughput snapshot for the time series, captured every {@code metricsWindowSecs}
     * seconds (default 10 s).
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ThroughputSnapshot {

        /** Milliseconds elapsed since the start of the measurement window. */
        private long elapsedMs;

        /** Operations per second observed in this window. */
        private double tps;

        /** TestProfile phase active at this moment. */
        private String phaseName;
    }

    /**
     * Per-query latency snapshot for the time series; shows how latency evolves across
     * load phases.
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

        /** Query identifier, matching {@code @SqlQuery.queryId}. */
        private String queryId;

        /** Number of executions during the measurement phases. */
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

        /** Executions per minute over the total measurement duration. */
        private double callsPerMinute;

        /**
         * Mean planner cost from {@code EXPLAIN ANALYZE} over the samples; compared against
         * the baseline to detect plan changes.
         */
        private double planCost;

        /**
         * Full execution plan text from the last {@code EXPLAIN ANALYZE} run for
         * this query. Complements {@code planCost} by showing structural changes
         * (e.g. index scan → seq scan) that a cost threshold alone cannot identify.
         */
        private String executionPlanText;

        /**
         * Percentage of operations within {@code maxResponseTimeMs}; 100.0 when the query has
         * no contract. Below {@code loadtest.sla.complianceThresholdPct} (default 95) the
         * verdict is FAIL.
         */
        private double slaComplianceRate;

        /**
         * Measured p95 as a percentage of {@code maxResponseTimeMs} (p95=86 ms with SLA=100 ms
         * → 86); 0 when the query has no contract. Above 80 % (default) the detector reports
         * {@code SLO_PROXIMITY}; at 50 % or more (default) a plan cost increase blocks as
         * {@code PLAN_CHANGED}.
         */
        private double slaRiskPct;

        /**
         * Latency per time window during measurement; exposes point-in-time spikes.
         */
        private List<LatencySnapshot> latencyTimeSeries;

        /**
         * FAIL when p95 exceeds {@code maxResponseTimeMs} or {@code slaComplianceRate} is below
         * the compliance threshold; PASS otherwise.
         */
        private Verdict verdict;

        /**
         * Failure reason when {@code verdict == FAIL}, {@code null} when PASS.
         * Example: "p95=320ms exceeds maxResponseTimeMs=200ms".
         */
        private String failReason;
    }
}
