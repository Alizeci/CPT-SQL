package escuelaing.edu.co.infrastructure;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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
 *   <li>Crear el esquema TPC-C adaptado (tablas warehouse, district, customer,
 *       orders, order_line, item, stock, new_order, history).</li>
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

    // -------------------------------------------------------------------------
    // API pública
    // -------------------------------------------------------------------------

    /**
     * Garantiza que la BD espejo esté disponible y su esquema TPC-C creado.
     * Llama a este método antes de iniciar el benchmark.
     *
     * @throws RuntimeException si no se puede establecer la conexión tras levantar Docker
     */
    public void provision() {
        ensureContainerRunning();
        waitForPostgres();
        try (Connection conn = openConnection()) {
            createSchema(conn);
            LOG.info("[MirrorDB] Esquema TPC-C listo en " + jdbcUrl());
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
    // Esquema TPC-C adaptado
    // -------------------------------------------------------------------------

    /**
     * Crea las tablas del esquema TPC-C si no existen.
     * La decisión de usar TPC-C (referencia: BenchmarkSQL-4) está documentada
     * en el contexto de diseño: representa patrones transaccionales OLTP reales.
     */
    private void createSchema(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("""
                CREATE TABLE IF NOT EXISTS warehouse (
                    w_id        SERIAL PRIMARY KEY,
                    w_name      VARCHAR(10),
                    w_street_1  VARCHAR(20),
                    w_street_2  VARCHAR(20),
                    w_city      VARCHAR(20),
                    w_state     CHAR(2),
                    w_zip       CHAR(9),
                    w_tax       NUMERIC(4,4),
                    w_ytd       NUMERIC(12,2)
                )
                """);

            st.execute("""
                CREATE TABLE IF NOT EXISTS district (
                    d_id        INTEGER,
                    d_w_id      INTEGER REFERENCES warehouse(w_id),
                    d_name      VARCHAR(10),
                    d_street_1  VARCHAR(20),
                    d_street_2  VARCHAR(20),
                    d_city      VARCHAR(20),
                    d_state     CHAR(2),
                    d_zip       CHAR(9),
                    d_tax       NUMERIC(4,4),
                    d_ytd       NUMERIC(12,2),
                    d_next_o_id INTEGER,
                    PRIMARY KEY (d_w_id, d_id)
                )
                """);

            st.execute("""
                CREATE TABLE IF NOT EXISTS customer (
                    c_id        INTEGER,
                    c_d_id      INTEGER,
                    c_w_id      INTEGER,
                    c_first     VARCHAR(16),
                    c_middle    CHAR(2),
                    c_last      VARCHAR(16),
                    c_street_1  VARCHAR(20),
                    c_street_2  VARCHAR(20),
                    c_city      VARCHAR(20),
                    c_state     CHAR(2),
                    c_zip       CHAR(9),
                    c_phone     CHAR(16),
                    c_since     TIMESTAMP,
                    c_credit    CHAR(2),
                    c_credit_lim NUMERIC(12,2),
                    c_discount  NUMERIC(4,4),
                    c_balance   NUMERIC(12,2),
                    c_ytd_payment NUMERIC(12,2),
                    c_payment_cnt INTEGER,
                    c_delivery_cnt INTEGER,
                    c_data      VARCHAR(500),
                    PRIMARY KEY (c_w_id, c_d_id, c_id)
                )
                """);

            st.execute("""
                CREATE TABLE IF NOT EXISTS item (
                    i_id    INTEGER PRIMARY KEY,
                    i_im_id INTEGER,
                    i_name  VARCHAR(24),
                    i_price NUMERIC(5,2),
                    i_data  VARCHAR(50)
                )
                """);

            st.execute("""
                CREATE TABLE IF NOT EXISTS stock (
                    s_i_id     INTEGER REFERENCES item(i_id),
                    s_w_id     INTEGER REFERENCES warehouse(w_id),
                    s_quantity SMALLINT,
                    s_dist_01  CHAR(24), s_dist_02 CHAR(24), s_dist_03 CHAR(24),
                    s_dist_04  CHAR(24), s_dist_05 CHAR(24), s_dist_06 CHAR(24),
                    s_dist_07  CHAR(24), s_dist_08 CHAR(24), s_dist_09 CHAR(24),
                    s_dist_10  CHAR(24),
                    s_ytd      NUMERIC(8,0),
                    s_order_cnt SMALLINT,
                    s_remote_cnt SMALLINT,
                    s_data     VARCHAR(50),
                    PRIMARY KEY (s_w_id, s_i_id)
                )
                """);

            st.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    o_id        INTEGER,
                    o_d_id      INTEGER,
                    o_w_id      INTEGER,
                    o_c_id      INTEGER,
                    o_entry_d   TIMESTAMP,
                    o_carrier_id INTEGER,
                    o_ol_cnt    SMALLINT,
                    o_all_local SMALLINT,
                    PRIMARY KEY (o_w_id, o_d_id, o_id)
                )
                """);

            st.execute("""
                CREATE TABLE IF NOT EXISTS new_order (
                    no_o_id  INTEGER,
                    no_d_id  INTEGER,
                    no_w_id  INTEGER,
                    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
                )
                """);

            st.execute("""
                CREATE TABLE IF NOT EXISTS order_line (
                    ol_o_id        INTEGER,
                    ol_d_id        INTEGER,
                    ol_w_id        INTEGER,
                    ol_number      INTEGER,
                    ol_i_id        INTEGER REFERENCES item(i_id),
                    ol_supply_w_id INTEGER,
                    ol_delivery_d  TIMESTAMP,
                    ol_quantity    SMALLINT,
                    ol_amount      NUMERIC(6,2),
                    ol_dist_info   CHAR(24),
                    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
                )
                """);

            st.execute("""
                CREATE TABLE IF NOT EXISTS history (
                    h_c_id   INTEGER,
                    h_c_d_id INTEGER,
                    h_c_w_id INTEGER,
                    h_d_id   INTEGER,
                    h_w_id   INTEGER,
                    h_date   TIMESTAMP,
                    h_amount NUMERIC(6,2),
                    h_data   VARCHAR(24)
                )
                """);
        }
        LOG.info("[MirrorDB] Tablas TPC-C verificadas/creadas.");
    }

    // -------------------------------------------------------------------------

    private String jdbcUrl() {
        return "jdbc:postgresql://" + host + ":" + port + "/" + db;
    }
}
