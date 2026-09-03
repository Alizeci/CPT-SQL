package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.infrastructure.dialect.DatabaseDialect;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
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

    private final DatabaseDialect adapter;

    public MirrorDatabaseProvisioner(DatabaseDialect adapter) {
        this.adapter = adapter;
    }

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
        waitForDatabase();
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

    // package-private for unit testing
    void ensureContainerRunning() {
        if (isContainerRunning()) {
            LOG.info("[MirrorDB] Container '" + containerName + "' is already running.");
            return;
        }
        try {
            ensureContainerRemovedIfUnhealthy();
        } catch (IOException | InterruptedException e) {
            LOG.warning("[MirrorDB] Could not inspect/remove stale container: " + e.getMessage());
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
        }
        LOG.info("[MirrorDB] Starting container '" + containerName + "'...");
        startContainer();
    }

    // package-private for unit testing (avoids real Docker calls in CI)
    boolean isContainerRunning() {
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

    // package-private for unit testing
    void ensureContainerRemovedIfUnhealthy() throws IOException, InterruptedException {
        Process inspect = new ProcessBuilder(
                "docker", "inspect", "-f", "{{.State.Status}}", containerName)
                .redirectErrorStream(true)
                .start();
        inspect.waitFor();
        String status = new String(inspect.getInputStream().readAllBytes()).strip();

        // empty or "Error" means the container does not exist — nothing to remove
        if (status.isEmpty() || status.contains("Error")) {
            return;
        }

        // container exists but is not running — remove it so docker run can succeed
        if (!status.equals("running")) {
            LOG.warning("[MirrorDB] Container '" + containerName +
                        "' exists with status='" + status + "'. Removing before recreate.");
            new ProcessBuilder("docker", "rm", "-f", containerName)
                    .redirectErrorStream(true)
                    .start()
                    .waitFor();
        }
    }

    // package-private for unit testing
    void startContainer() {
        try {
            List<String> cmd = new ArrayList<>();
            cmd.add("docker"); cmd.add("run"); cmd.add("--name"); cmd.add(containerName);
            adapter.getDockerEnv(db, user, password)
                   .forEach((k, v) -> { cmd.add("-e"); cmd.add(k + "=" + v); });
            cmd.add("-p"); cmd.add(port + ":" + adapter.getContainerPort());
            cmd.add("-d"); cmd.add(adapter.getDockerImage());

            Process run = new ProcessBuilder(cmd)
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

    private void waitForDatabase() {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            try (Connection conn = openConnection();
                 var ps = conn.prepareStatement(adapter.getHealthCheckQuery())) {
                ps.execute();
                return;
            } catch (SQLException ignored) {
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for database");
                }
            }
        }
        throw new RuntimeException(adapter.getEngineName()
                + " did not respond within 30s at " + adapter.buildJdbcUrl(host, port, db));
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
        return adapter.buildJdbcUrl(host, port, db);
    }
}
