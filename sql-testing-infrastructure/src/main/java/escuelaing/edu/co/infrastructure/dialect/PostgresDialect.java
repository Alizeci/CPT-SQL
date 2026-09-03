package escuelaing.edu.co.infrastructure.dialect;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.stereotype.Component;

import java.io.StringReader;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

/** PostgreSQL 17 implementation of {@link DatabaseDialect}. Active by default. */
@Component
public class PostgresDialect implements DatabaseDialect {

    @Override
    public String getEngineName() { return "postgres"; }

    @Override
    public String getDockerImage() { return "postgres:17"; }

    @Override
    public int getContainerPort() { return 5432; }

    @Override
    public Map<String, String> getDockerEnv(String db, String user, String password) {
        return Map.of(
                "POSTGRES_DB",       db,
                "POSTGRES_USER",     user,
                "POSTGRES_PASSWORD", password
        );
    }

    @Override
    public String buildJdbcUrl(String host, int port, String db) {
        return "jdbc:postgresql://" + host + ":" + port + "/" + db;
    }

    @Override
    public String getHealthCheckQuery() { return "SELECT 1"; }

    @Override
    public long bulkInsert(Connection conn, String table, List<String> columns, String csvData)
            throws Exception {
        String copySql = "COPY " + table + " (" + String.join(", ", columns)
                + ") FROM STDIN WITH (FORMAT CSV)";
        CopyManager cm = new CopyManager(conn.unwrap(BaseConnection.class));
        return cm.copyIn(copySql, new StringReader(csvData));
    }

    @Override
    public String buildExplainAnalyze(String sql) {
        return "EXPLAIN ANALYZE " + sql;
    }

    /** Parses {@code cost=X..Y} on the root plan node and returns Y (total cost). */
    @Override
    public double parsePlanCost(String explainOutput) {
        if (explainOutput == null || explainOutput.isBlank()) return 0.0;
        int idx = explainOutput.indexOf("cost=");
        if (idx < 0) return 0.0;
        try {
            String sub = explainOutput.substring(idx + 5);
            int dotDot = sub.indexOf("..");
            int space  = sub.indexOf(' ');
            if (dotDot < 0 || space < 0) return 0.0;
            return Double.parseDouble(sub.substring(dotDot + 2, space));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    /**
     * Extracts the root node type from a PostgreSQL EXPLAIN ANALYZE output.
     * Example: {@code "->  Index Scan using idx on t  (cost=..."} → {@code "Index Scan using idx on t"}.
     */
    @Override
    public String parsePlanRootNode(String explainOutput) {
        if (explainOutput == null || explainOutput.isBlank()) return "";
        String firstLine = explainOutput.strip().lines().findFirst().orElse("");
        int paren = firstLine.indexOf('(');
        String node = paren > 0 ? firstLine.substring(0, paren) : firstLine;
        return node.replace("->", "").strip();
    }
}
