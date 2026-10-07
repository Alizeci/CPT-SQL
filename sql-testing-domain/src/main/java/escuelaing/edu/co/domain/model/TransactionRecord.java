package escuelaing.edu.co.domain.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Map;

/**
 * Single SQL execution recorded by the JDBC wrapper in the capture environment; the
 * atomic unit aggregated into the {@link LoadProfile}.
 */
@Data
@Builder
public class TransactionRecord {

    private String queryId;
    private String sql;
    /** Execution latency in milliseconds. */
    private long latencyMs;
    private Instant timestamp;

    /** EXPLAIN ANALYZE output, or {@code null} when plan capture was not requested. */
    private String executionPlan;

    /** Rows affected by INSERT/UPDATE/DELETE; {@code 0} for SELECT. */
    private long rowCount;

    /**
     * Sanitized row data (business feature columns only, no PII); {@code null} for write
     * operations. Input to the differentially private release; never persisted by default.
     */
    private Map<String, Object> sanitizedData;
}
