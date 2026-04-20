package escuelaing.edu.co.infrastructure.capture;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Contract for sanitizing data captured by the JDBC wrapper.
 *
 * <p>Implementations define which columns are safe to retain in captured
 * {@link TransactionRecord}s. The infrastructure module has no knowledge
 * of the application schema; each deployment provides its own implementation.</p>
 */
public interface SanitizationStrategy {

    /**
     * Extracts safe columns from the current row of a {@link ResultSet}.
     *
     * @param rs ResultSet positioned at the current row
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
