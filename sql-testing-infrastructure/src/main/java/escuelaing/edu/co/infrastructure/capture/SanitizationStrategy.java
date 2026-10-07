package escuelaing.edu.co.infrastructure.capture;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Redacts sensitive values in the rows captured by {@link JdbcWrapper}.
 *
 * <p>Implementations decide which columns are safe to retain in captured
 * {@link escuelaing.edu.co.domain.model.TransactionRecord}s; the infrastructure module has no
 * knowledge of the application schema, so each deployment provides its own. Sanitization
 * is a heuristic filter with no formal privacy guarantee: retained values feed the
 * row-level differentially private release and, only when {@code CPT_DP_KEEP_SANITIZED_ROWS=true},
 * appear raw in the load profile.</p>
 */
public interface SanitizationStrategy {

    /**
     * Extracts safe columns from the current row of a {@link ResultSet}.
     *
     * @param rs ResultSet positioned at the current row; must not be advanced
     * @return map of {@code columnName → value} containing only safe columns; never {@code null}
     * @throws SQLException if an error occurs reading the ResultSet
     */
    Map<String, Object> sanitize(ResultSet rs) throws SQLException;

    /**
     * Filters safe columns from an already-extracted row map.
     *
     * @param rawRow map with all fields from a row
     * @return subset containing only safe columns
     */
    Map<String, Object> sanitize(Map<String, Object> rawRow);
}
