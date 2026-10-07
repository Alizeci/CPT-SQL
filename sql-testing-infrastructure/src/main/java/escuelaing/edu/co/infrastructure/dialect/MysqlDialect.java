package escuelaing.edu.co.infrastructure.dialect;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * MySQL 8+ dialect skeleton: a starting point for adopting CPT-SQL on MySQL.
 * Methods marked TODO throw {@link UnsupportedOperationException} until implemented.
 * Not registered as a bean and not used by the current pipeline; see
 * {@link DatabaseDialect} for the contract and {@link PostgresDialect} for a reference
 * implementation.
 */
public class MysqlDialect implements DatabaseDialect {

    @Override
    public String getEngineName() { return "mysql"; }

    @Override
    public String getDockerImage() { return "mysql:8"; }

    @Override
    public int getContainerPort() { return 3306; }

    @Override
    public Map<String, String> getDockerEnv(String db, String user, String password) {
        return Map.of(
                "MYSQL_DATABASE",      db,
                "MYSQL_USER",          user,
                "MYSQL_PASSWORD",      password,
                "MYSQL_ROOT_PASSWORD", password
        );
    }

    @Override
    public String buildJdbcUrl(String host, int port, String db) {
        return "jdbc:mysql://" + host + ":" + port + "/" + db
                + "?allowLoadLocalInfile=true&useSSL=false&serverTimezone=UTC";
    }

    @Override
    public String getHealthCheckQuery() { return "SELECT 1"; }

    @Override
    public long bulkInsert(Connection conn, String table, List<String> columns, String csvData) {
        // TODO: implement with LOAD DATA LOCAL INFILE.
        // Write csvData to a temp file and execute:
        // LOAD DATA LOCAL INFILE '<path>' INTO TABLE <table> FIELDS TERMINATED BY ',' (col1, col2, ...)
        // Contract: DatabaseDialect.bulkInsert(); reference implementation: PostgresDialect.
        throw new UnsupportedOperationException("MysqlDialect.bulkInsert not yet implemented. See the DatabaseDialect contract and PostgresDialect.");
    }

    @Override
    public String buildExplainAnalyze(String sql) {
        // MySQL 8.0.18+ supports EXPLAIN ANALYZE natively.
        return "EXPLAIN ANALYZE " + sql;
    }

    @Override
    public double parsePlanCost(String explainOutput) {
        // TODO: parse MySQL EXPLAIN ANALYZE output, which differs from PostgreSQL's "cost=X..Y" format.
        // Use EXPLAIN FORMAT=JSON to access query_cost in the root block.
        throw new UnsupportedOperationException("MysqlDialect.parsePlanCost not yet implemented. See the DatabaseDialect contract and PostgresDialect.");
    }

    @Override
    public String parsePlanRootNode(String explainOutput) {
        // TODO: extract the root operator from MySQL EXPLAIN ANALYZE output.
        // The first-line format differs from PostgreSQL's "->  Index Scan..." pattern.
        throw new UnsupportedOperationException("MysqlDialect.parsePlanRootNode not yet implemented. See the DatabaseDialect contract and PostgresDialect.");
    }
}
