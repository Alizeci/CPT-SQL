package escuelaing.edu.co.domain.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Map;

/**
 * Single SQL execution captured by the JDBC wrapper (Phase 2).
 * The atomic unit used to build the {@link LoadProfile}.
 */
@Data
@Builder
public class TransactionRecord {

    private String queryId;
    private String sql;
    private long latencyMs;
    private Instant timestamp;

    /** EXPLAIN ANALYZE output, or {@code null} when plan capture was not requested. */
    private String executionPlan;

    /** Rows affected by INSERT/UPDATE/DELETE; {@code 0} for SELECT. */
    private long rowCount;

    /**
     * Sanitized row data (Feature columns only, no PII).
     * {@code null} for write operations.
     */
    private Map<String, Object> sanitizedData;
}
