package escuelaing.edu.co.infrastructure.benchmark;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

/**
 * Provisions the mirror database used by the Phase 3 benchmark engine.
 *
 * <p>Responsibilities: start the PostgreSQL Docker container if not running,
 * wait for it to accept connections, and apply the user's DDL script to create
 * the schema. Exposes an open {@link Connection} to other Phase 3 components.</p>
 *
 * <p>{@code loadtest.mirror.schema.script} must point to the application's DDL
 * file (tables, views, indexes). If missing, provisioning fails explicitly —
 * no default schema is assumed.</p>
 *
 * <pre>
 * loadtest.mirror.host=localhost
 * loadtest.mirror.port=5433
 * loadtest.mirror.db=mirror
 * loadtest.mirror.user=mirror
 * loadtest.mirror.password=mirror
 * loadtest.mirror.container=cpt-sql-mirror
 * loadtest.mirror.schema.script=path/to/schema.sql
 * </pre>
 */
@Component
public class MirrorDatabaseProvisioner {

    private static final Logger LOG = Logger.getLogger(MirrorDatabaseProvisioner.class.getName());

    @Value("${loadtest.mirror.host:localhost}")
    private String host;

    @Value("${loadtest.mirror.port:5433}")
    private int port;

    @Value("${loadtest.mirror.db:mirror}")
    private String db;

    @Value("${loadtest.mirror.user:mirror}")
    private String user;

    @Value("${loadtest.mirror.password:mirror}")
    private String password;

    @Value("${loadtest.mirror.container:cpt-sql-mirror}")
    private String containerName;

    @Value("${loadtest.mirror.schema.script:}")
    private String schemaScript;

    // Public API

    /**
     * Ensures the mirror database is running and its schema is applied.
     * Call this before starting the benchmark.
     */
    public void provision() {
        ensureContainerRunning();
        waitForPostgres();
        try (Connection conn = openConnection()) {
            createSchema(conn);
        } catch (SQLException e) {
            throw new RuntimeException("Mirror database provisioning failed", e);
        }
    }

    public Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(), user, password);
    }

    // Docker

    private void ensureContainerRunning() {
        if (isContainerRunning()) {
            LOG.info("[MirrorDB] Container '" + containerName + "' is already running.");
            return;
        }
        LOG.info("[MirrorDB] Starting container '" + containerName + "'...");
        startContainer();
    }

    private boolean isContainerRunning() {
        try {
            Process check = new ProcessBuilder(
                    "docker", "inspect", "-f", "{{.State.Running}}", containerName)
                    .redirectErrorStream(true)
                    .start();
            String output = new String(check.getInputStream().readAllBytes()).trim();
            check.waitFor();
            return "true".equals(output);
        } catch (Exception e) {
            return false;
        }
    }

    private void startContainer() {
        try {
            Process run = new ProcessBuilder(
                    "docker", "run", "--name", containerName,
                    "-e", "POSTGRES_DB=" + db,
                    "-e", "POSTGRES_USER=" + user,
                    "-e", "POSTGRES_PASSWORD=" + password,
                    "-p", port + ":5432",
                    "-d", "postgres:17")
                    .redirectErrorStream(true)
                    .start();
            int exit = run.waitFor();
            if (exit != 0) {
                String out = new String(run.getInputStream().readAllBytes());
                throw new RuntimeException("docker run failed (exit=" + exit + "): " + out);
            }
            LOG.info("[MirrorDB] Container started.");
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Docker container", e);
        }
    }

    private void waitForPostgres() {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                openConnection().close();
                return;
            } catch (SQLException ignored) {
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for PostgreSQL");
                }
            }
        }
        throw new RuntimeException("PostgreSQL did not respond within 30s at " + jdbcUrl());
    }

    // Schema

    private void createSchema(Connection conn) throws SQLException {
        if (schemaScript == null || schemaScript.isBlank()) {
            throw new IllegalStateException(
                    "[MirrorDB] 'loadtest.mirror.schema.script' is not configured. " +
                    "Provide the path to your application's DDL script (tables, views, indexes).");
        }
        executeScript(conn, schemaScript);
    }

    /**
     * Executes a DDL script against the mirror database.
     *
     * <p>Statements are split on {@code ;} and executed independently so that
     * a single failure (e.g. "already exists") does not abort the rest of the schema.</p>
     */
    private void executeScript(Connection conn, String scriptPath) throws SQLException {
        LOG.info("[MirrorDB] Applying DDL script: " + scriptPath);
        String content;
        try {
            content = Files.readString(Path.of(scriptPath));
        } catch (IOException e) {
            throw new RuntimeException("Cannot read DDL script: " + scriptPath, e);
        }

        int executed = 0;
        try (Statement st = conn.createStatement()) {
            for (String raw : content.split(";")) {
                String stmt = raw.lines()
                        .filter(line -> !line.trim().startsWith("--"))
                        .reduce("", (a, b) -> a + "\n" + b)
                        .trim();
                if (stmt.isEmpty()) continue;
                try {
                    st.execute(stmt);
                    executed++;
                } catch (SQLException e) {
                    String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
                    if (!msg.contains("already exists") && !msg.contains("duplicate")) {
                        LOG.warning("[MirrorDB] Statement skipped (" + e.getMessage() + "): "
                                + stmt.substring(0, Math.min(80, stmt.length())));
                    }
                }
            }
        }
        LOG.info("[MirrorDB] DDL applied: " + executed + " statements executed.");
    }

    private String jdbcUrl() {
        return "jdbc:postgresql://" + host + ":" + port + "/" + db;
    }
}
