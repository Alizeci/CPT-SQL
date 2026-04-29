package escuelaing.edu.co.domain.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.List;

/**
 * Degradation report produced by {@code DegradationDetector} (Phase 3).
 *
 * <p>Details which queries failed, the type of degradation detected, and the
 * observed values versus the reference thresholds.</p>
 *
 * <p>This is the decision artifact the CI/CD pipeline uses to block or approve
 * a merge.</p>
 */
@Data
@Builder
public class DegradationReport {

    /** Timestamp at which the report was generated. */
    private Instant generatedAt;

    /** Name of the load profile that produced this report. */
    private String profileName;

    /** SHA of the evaluated commit. */
    private String commitSha;

    /** {@code true} if at least one degradation blocks the merge. */
    private boolean hasDegradations;

    /** List of detected degradations (empty when {@code hasDegradations == false}). */
    private List<Degradation> degradations;

    // -------------------------------------------------------------------------

    /** Type of detected degradation. */
    public enum DegradationType {

        /** Measured p95 exceeds the {@code maxResponseTimeMs} declared in {@code @Req}. */
        P95_EXCEEDED,

        /**
         * The execution plan changed and the query has {@code allowPlanChange = false}
         * in {@code @Req}.
         */
        PLAN_CHANGED,

        /**
         * Measured p95 exceeds the baseline p95 by more than the tolerance margin
         * (10 % by default).
         */
        BASELINE_EXCEEDED,

        /**
         * Measured p95 exceeds the internal SLA proximity threshold (80 % by default).
         * The query is in the risk zone: a normal environment fluctuation could cross
         * the SLA in the next run. Blocks the merge even though the SLA has not yet
         * been formally violated.
         */
        SLO_PROXIMITY
    }

    /** Detail of a single detected degradation. */
    @Data
    @Builder
    public static class Degradation {

        /** Query that exhibited the degradation. */
        private String queryId;

        /** Type of degradation. */
        private DegradationType type;

        /**
         * Value observed during the benchmark.
         * <ul>
         *   <li>{@link DegradationType#P95_EXCEEDED} → measured p95 in ms</li>
         *   <li>{@link DegradationType#PLAN_CHANGED} → cost of the new plan</li>
         *   <li>{@link DegradationType#BASELINE_EXCEEDED} → measured p95 in ms</li>
         * </ul>
         */
        private double observedValue;

        /**
         * Threshold that was exceeded.
         * <ul>
         *   <li>{@link DegradationType#P95_EXCEEDED} → {@code maxResponseTimeMs} from {@code @Req}</li>
         *   <li>{@link DegradationType#PLAN_CHANGED} → baseline plan cost</li>
         *   <li>{@link DegradationType#BASELINE_EXCEEDED} → baseline p95 × 1.10</li>
         * </ul>
         */
        private double thresholdValue;

        /** Human-readable message for the developer. */
        private String description;
    }
}
