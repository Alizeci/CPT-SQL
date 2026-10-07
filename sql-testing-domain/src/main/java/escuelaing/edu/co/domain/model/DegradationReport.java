package escuelaing.edu.co.domain.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.List;

/**
 * Degradation report produced by {@code DegradationDetector}: which queries degraded,
 * under which criterion, and the observed value versus the threshold.
 *
 * <p>This is the artifact the gate uses to block or approve a merge: {@code P95_EXCEEDED},
 * {@code SLO_PROXIMITY} and {@code PLAN_CHANGED} block; {@code BASELINE_EXCEEDED} is a
 * warning only.</p>
 */
@Data
@Builder
public class DegradationReport {

    /** Timestamp at which the report was generated. */
    private Instant generatedAt;

    /** Run label of the evaluated benchmark ({@code loadtest.benchmark.profileName}). */
    private String profileName;

    /** SHA of the evaluated commit. */
    private String commitSha;

    /**
     * {@code true} if {@code degradations} is non-empty, including non-blocking
     * {@code BASELINE_EXCEEDED} warnings.
     */
    private boolean hasDegradations;

    /** List of detected degradations (empty when {@code hasDegradations == false}). */
    private List<Degradation> degradations;

    /**
     * "COMPARABLE", "MISSING" (first run) or "INCOMPATIBLE: reason". When not comparable,
     * baseline-relative criteria (BASELINE_EXCEEDED, PLAN_CHANGED) are skipped and only
     * the contract criteria (P95_EXCEEDED, SLO_PROXIMITY) are evaluated.
     */
    private String baselineStatus;

    /** Type of detected degradation. */
    public enum DegradationType {

        /** Measured p95 exceeds the {@code maxResponseTimeMs} declared in {@code @Req}. */
        P95_EXCEEDED,

        /**
         * The query has {@code allowPlanChange = false} in {@code @Req}, its plan cost exceeds
         * the baseline cost by more than the tolerance (20 % by default), and its p95 uses at
         * least 50 % of the SLA (by default). Below that SLA share the cost increase is reported
         * as {@code BASELINE_EXCEEDED} instead. Blocking.
         */
        PLAN_CHANGED,

        /**
         * Measured p95 exceeds the baseline p95 by more than the tolerance margin (10 % by
         * default) and by at least 10 ms (by default); also used for plan cost increases
         * below the {@code PLAN_CHANGED} SLA share. Warning only; dropped for queries that
         * already have a blocking degradation.
         */
        BASELINE_EXCEEDED,

        /**
         * Measured p95 is within the SLA but above the proximity threshold (80 % of the SLA
         * by default): a normal environment fluctuation could cross the SLA in the next run.
         * Blocking, even though the SLA has not been violated.
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
         *   <li>{@link DegradationType#P95_EXCEEDED}, {@link DegradationType#SLO_PROXIMITY}
         *       → measured p95 in ms</li>
         *   <li>{@link DegradationType#PLAN_CHANGED} → cost of the new plan</li>
         *   <li>{@link DegradationType#BASELINE_EXCEEDED} → measured p95 in ms, or the new plan
         *       cost for a plan cost warning</li>
         * </ul>
         */
        private double observedValue;

        /**
 * Reference value for the criterion.
         * <ul>
         *   <li>{@link DegradationType#P95_EXCEEDED} → {@code maxResponseTimeMs} from {@code @Req}</li>
         *   <li>{@link DegradationType#SLO_PROXIMITY} → {@code maxResponseTimeMs} × proximity
         *       fraction</li>
         *   <li>{@link DegradationType#PLAN_CHANGED} → baseline plan cost × (1 + tolerance)</li>
         *   <li>{@link DegradationType#BASELINE_EXCEEDED} → baseline p95 (not the tolerance-scaled
         *       value), or the baseline plan cost for a plan cost warning</li>
         * </ul>
         */
        private double thresholdValue;

        /** Human-readable message for the developer. */
        private String description;
    }
}
