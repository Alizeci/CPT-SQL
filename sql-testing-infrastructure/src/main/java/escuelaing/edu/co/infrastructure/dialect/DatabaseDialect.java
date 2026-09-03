package escuelaing.edu.co.infrastructure.dialect;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Extension point for database engine support.
 * Isolates engine-specific concerns from provisioning, synthetic data generation,
 * and execution plan analysis.
 */
public interface DatabaseDialect {

    /** Short engine name, e.g. "postgres" or "mysql". */
    String getEngineName();

    /** Docker image to start, e.g. "postgres:17" or "mysql:8". */
    String getDockerImage();

    /** Internal container port for the engine. */
    int getContainerPort();

    /** Environment variables for docker run. */
    Map<String, String> getDockerEnv(String db, String user, String password);

    /** Engine-specific JDBC URL. */
    String buildJdbcUrl(String host, int port, String db);

    /** Minimal query to verify the engine accepts connections. */
    String getHealthCheckQuery();

    /**
     * Engine-specific bulk insert.
     * PostgreSQL: COPY FROM STDIN. MySQL: LOAD DATA LOCAL INFILE.
     * @return number of rows inserted.
     */
    long bulkInsert(Connection conn, String table, List<String> columns, String csvData) throws Exception;

    /** Engine-specific EXPLAIN ANALYZE syntax. */
    String buildExplainAnalyze(String sql);

    /** Extracts the total plan cost from an EXPLAIN ANALYZE output. */
    double parsePlanCost(String explainOutput);

    /** Extracts the root node type from an EXPLAIN ANALYZE output. */
    String parsePlanRootNode(String explainOutput);
}
