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
 * Provisiona la base de datos espejo que el motor de benchmark (Fase 3) usa
 * para ejecutar las pruebas de carga sin tocar producción.
 *
 * <h3>Responsabilidades</h3>
 * <ol>
 *   <li>Verificar si el contenedor Docker con PostgreSQL está corriendo;
 *       si no, levantarlo vía {@code docker run}.</li>
 *   <li>Crear el esquema en la BD espejo ejecutando el script DDL configurado
 *       en {@code loadtest.mirror.schema.script} — modo <b>agnóstico</b> al
 *       schema del usuario.</li>
 *   <li>Exponer una {@link Connection} lista para usar a los demás componentes
 *       de la Fase 3.</li>
 * </ol>
 *
 * <h3>Configuración (application.properties)</h3>
 * <pre>
 * loadtest.mirror.host=localhost
 * loadtest.mirror.port=5433
 * loadtest.mirror.db=mirror
 * loadtest.mirror.user=mirror
 * loadtest.mirror.password=mirror
 * loadtest.mirror.container=sql-load-testing-mirror
 *
 * # Ruta al script DDL del usuario (tablas, vistas, índices). Obligatorio.
 * loadtest.mirror.schema.script=
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

    @Value("${loadtest.mirror.container:sql-load-testing-mirror}")
    private String containerName;

    @Value("${loadtest.mirror.schema.script:}")
    private String schemaScript;

    // -------------------------------------------------------------------------
    // API pública
    // -------------------------------------------------------------------------

    /**
     * Garantiza que la BD espejo esté disponible y su esquema creado.
     * Llama a este método antes de iniciar el benchmark.
     *
     * @throws RuntimeException si no se puede establecer la conexión tras levantar Docker
     */
    public void provision() {
        ensureContainerRunning();
        waitForPostgres();
        try (Connection conn = openConnection()) {
            createSchema(conn);
        } catch (SQLException e) {
            throw new RuntimeException("No se pudo provisionar la BD espejo", e);
        }
    }

    /**
     * Abre y retorna una nueva {@link Connection} a la BD espejo.
     * El llamador es responsable de cerrarla (try-with-resources).
     */
    public Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(), user, password);
    }

    // -------------------------------------------------------------------------
    // Docker
    // -------------------------------------------------------------------------

    private void ensureContainerRunning() {
        if (isContainerRunning()) {
            LOG.info("[MirrorDB] Contenedor '" + containerName + "' ya está corriendo.");
            return;
        }
        LOG.info("[MirrorDB] Levantando contenedor '" + containerName + "'...");
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
                    "-d", "postgres:15")
                    .redirectErrorStream(true)
                    .start();
            int exit = run.waitFor();
            if (exit != 0) {
                String out = new String(run.getInputStream().readAllBytes());
                throw new RuntimeException("docker run falló (exit=" + exit + "): " + out);
            }
            LOG.info("[MirrorDB] Contenedor iniciado.");
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error al iniciar el contenedor Docker", e);
        }
    }

    /** Espera hasta que PostgreSQL acepte conexiones (máx. 30 s). */
    private void waitForPostgres() {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                openConnection().close();
                return;
            } catch (SQLException ignored) {
                try { Thread.sleep(1_000); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrumpido esperando PostgreSQL");
                }
            }
        }
        throw new RuntimeException("PostgreSQL no respondió en 30 s en " + jdbcUrl());
    }

    // -------------------------------------------------------------------------
    // Schema — agnóstico al usuario
    // -------------------------------------------------------------------------

    /**
     * Crea el schema en la BD espejo ejecutando el script DDL del usuario.
     *
     * @throws IllegalStateException si {@code loadtest.mirror.schema.script} no está configurado
     */
    private void createSchema(Connection conn) throws SQLException {
        if (schemaScript == null || schemaScript.isBlank()) {
            throw new IllegalStateException(
                    "[MirrorDB] Propiedad 'loadtest.mirror.schema.script' no configurada. " +
                    "Provee la ruta al script DDL de tu aplicación (tablas, vistas, índices).");
        }
        executeUserScript(conn, schemaScript);
    }

    /**
     * Ejecuta el script DDL del usuario sobre la BD espejo.
     *
     * <p>El script puede contener múltiples sentencias separadas por {@code ;}.
     * Las líneas vacías y los comentarios SQL ({@code --}) se omiten.
     * Cada sentencia se ejecuta de forma independiente para que un fallo
     * individual no aborte el resto del schema.</p>
     *
     * @param conn       conexión a la BD espejo
     * @param scriptPath ruta absoluta o relativa al archivo DDL
     */
    private void executeUserScript(Connection conn, String scriptPath) throws SQLException {
        LOG.info("[MirrorDB] Ejecutando script DDL del usuario: " + scriptPath);
        String content;
        try {
            content = Files.readString(Path.of(scriptPath));
        } catch (IOException e) {
            throw new RuntimeException("No se pudo leer el script DDL: " + scriptPath, e);
        }

        String[] statements = content.split(";");
        int executed = 0;
        try (Statement st = conn.createStatement()) {
            for (String raw : statements) {
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
                        LOG.warning("[MirrorDB] Sentencia omitida (" + e.getMessage() + "): "
                                + stmt.substring(0, Math.min(80, stmt.length())));
                    }
                }
            }
        }
        LOG.info("[MirrorDB] Script DDL ejecutado: " + executed + " sentencias aplicadas.");
    }

    private String jdbcUrl() {
        return "jdbc:postgresql://" + host + ":" + port + "/" + db;
    }
}
