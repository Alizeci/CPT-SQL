package escuelaing.edu.co.domain.model;

import lombok.Builder;
import lombok.Data;

/**
 * One query contract as written to {@code queries.json} by the annotation processor: the
 * {@code @SqlQuery} identity plus its {@code @Req} contract.
 *
 * <p>When {@code hasReq} is {@code false}, {@code maxResponseTimeMs} is {@code -1},
 * {@code priority} is {@code "NONE"} and {@code allowPlanChange} is {@code true}.</p>
 */
@Data
@Builder
public class QueryEntry {
    private String queryId;
    private String className;
    private String methodName;
    private String queryDescription;
    private boolean hasReq;
    /** p95 SLA in milliseconds; {@code -1} when there is no contract. */
    private long maxResponseTimeMs;
    /** {@code HIGH}, {@code MEDIUM}, {@code LOW}, or {@code NONE} without a contract. */
    private String priority;
    private String reqDescription;
    /** {@code false} enables {@code PLAN_CHANGED} detection for this query. */
    private boolean allowPlanChange;
}