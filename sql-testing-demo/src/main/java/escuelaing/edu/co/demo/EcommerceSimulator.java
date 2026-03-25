package escuelaing.edu.co.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.infrastructure.analysis.QueryRegistryLoader;
import escuelaing.edu.co.infrastructure.capture.CaptureToggle;
import escuelaing.edu.co.infrastructure.capture.JdbcWrapper;
import escuelaing.edu.co.infrastructure.capture.SanitizationStrategy;
import escuelaing.edu.co.infrastructure.capture.LoadProfileBuilder;
import escuelaing.edu.co.infrastructure.capture.MetricsBuffer;
import escuelaing.edu.co.infrastructure.capture.SamplingFilter;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Simulador de tráfico e-commerce — Java puro, sin Spring.
 *
 * <p>Demuestra cómo cualquier aplicación Java con JDBC puro puede adoptar
 * CPT-SQL sin cambiar su stack tecnológico:</p>
 * <ol>
 *   <li>Instancia {@link JdbcWrapper} manualmente (sin contenedor Spring).</li>
 *   <li>Simula las 5 queries del repositorio durante {@value #SIMULATION_SECS} s.</li>
 *   <li>Construye el {@link LoadProfile} y lo persiste en {@code load-profile.json}.</li>
 * </ol>
 *
 * <h3>Uso</h3>
 * <pre>
 * ./gradlew :sql-testing-demo:runSimulator
 * </pre>
 *
 * <h3>Variables de entorno</h3>
 * <pre>
 * DB_URL       (default: jdbc:postgresql://localhost:5432/ecommerce_demo)
 * DB_USER      (default: demo)
 * DB_PASSWORD  (default: demo)
 * </pre>
 */
public class EcommerceSimulator {

    private static final Logger LOG = Logger.getLogger(EcommerceSimulator.class.getName());

    private static final int SIMULATION_SECS = 60;
    private static final int THINK_TIME_MS   = 100;

    private static final String[] CATEGORIES =
            {"electronics", "clothing", "books", "sports", "home"};

    public static void main(String[] args) throws Exception {
        String url  = env("DB_URL",      "jdbc:postgresql://localhost:5432/ecommerce_demo");
        String user = env("DB_USER",     "demo");
        String pass = env("DB_PASSWORD", "demo");

        // -------------------------------------------------------------------------
        // 1. Wiring manual de CPT-SQL — sin Spring
        // -------------------------------------------------------------------------
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        QueryRegistryLoader registry = new QueryRegistryLoader(mapper);
        registry.load();                        // equivalente a @PostConstruct

        CaptureToggle toggle = new CaptureToggle();
        MetricsBuffer buffer = new MetricsBuffer();
        buffer.start();                         // equivalente a @PostConstruct

        SamplingFilter       filter      = new SamplingFilter(registry);
        SanitizationStrategy sanitization = new SanitizationStrategy();
        JdbcWrapper          wrapper      = new JdbcWrapper(filter, buffer, toggle, sanitization);

        // -------------------------------------------------------------------------
        // 2. Conexión, schema y seed data
        // -------------------------------------------------------------------------
        LOG.info("[Simulator] Conectando a " + url);
        try (Connection raw = DriverManager.getConnection(url, user, pass)) {
            applySchema(raw);
            insertSeedData(raw);

            Connection conn = wrapper.wrap(raw);
            EcommerceRepository repo = new EcommerceRepository(conn);
            Random rng = new Random(42);

            // -------------------------------------------------------------------------
            // 3. Simulación de tráfico — distribución proporcional a ProductRepository
            //    searchByCategory  ~60 %   (query más frecuente del catálogo)
            //    getProductDetail  ~20 %   (detalle de producto)
            //    checkInventory    ~10 %   (verificación de stock pre-checkout)
            //    createOrder        ~5 %   (transacción de compra)
            //    updateInventory    ~5 %   (ajuste de stock post-venta)
            // -------------------------------------------------------------------------
            LOG.info("[Simulator] Simulando tráfico durante " + SIMULATION_SECS + " s...");
            long endMs = System.currentTimeMillis() + (long) SIMULATION_SECS * 1_000;

            while (System.currentTimeMillis() < endMs) {
                int    productId  = rng.nextInt(500) + 1;
                int    customerId = rng.nextInt(200) + 1;
                String category   = CATEGORIES[rng.nextInt(CATEGORIES.length)];

                repo.searchByCategory(category);
                repo.getProductDetail(productId);

                if (rng.nextInt(5) == 0) {
                    repo.checkInventory(productId);
                }

                if (rng.nextInt(10) == 0) {
                    int orderId = repo.createOrder(customerId);
                    if (orderId > 0) {
                        repo.updateInventory(productId, -1);
                    }
                }

                Thread.sleep(THINK_TIME_MS);
            }
        }

        // -------------------------------------------------------------------------
        // 4. Construir LoadProfile y persistir
        // -------------------------------------------------------------------------
        buffer.stop();
        LoadProfileBuilder builder = new LoadProfileBuilder(buffer);
        LoadProfile profile = builder.build();

        Path out = Path.of("load-profile.json");
        mapper.writerWithDefaultPrettyPrinter().writeValue(out.toFile(), profile);

        LOG.info("[Simulator] load-profile.json guardado — "
                + profile.getTotalSamples() + " muestras, "
                + profile.getQueries().size() + " queries capturadas.");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void applySchema(Connection conn) throws Exception {
        String script = Files.readString(
                Path.of("sql-testing-demo/src/main/resources/schema-ecommerce.sql"));
        try (Statement st = conn.createStatement()) {
            for (String stmt : script.split(";")) {
                String s = stmt.strip();
                if (!s.isEmpty()) {
                    try { st.execute(s); } catch (SQLException ignored) {}
                }
            }
        }
        LOG.info("[Simulator] Schema aplicado.");
    }

    private static void insertSeedData(Connection conn) throws SQLException {
        try (PreparedStatement chk = conn.prepareStatement(
                "SELECT COUNT(*) FROM products");
             ResultSet rs = chk.executeQuery()) {
            if (rs.next() && rs.getLong(1) > 0) {
                LOG.info("[Simulator] Seed data ya existe — omitiendo inserción.");
                return;
            }
        }

        Random rng = new Random(42);
        conn.setAutoCommit(false);

        try (PreparedStatement pc = conn.prepareStatement(
                "INSERT INTO customers(name, email, tier) VALUES(?, ?, ?) ON CONFLICT DO NOTHING");
             PreparedStatement pp = conn.prepareStatement(
                "INSERT INTO products(name, category, price, stock_quantity, rating, active) " +
                "VALUES(?, ?, ?, ?, ?, true) ON CONFLICT DO NOTHING")) {

            for (int i = 1; i <= 200; i++) {
                pc.setString(1, "Customer " + i);
                pc.setString(2, "c" + i + "@demo.test");
                pc.setString(3, "STANDARD");
                pc.addBatch();
            }
            pc.executeBatch();

            for (int i = 1; i <= 500; i++) {
                String cat   = CATEGORIES[rng.nextInt(CATEGORIES.length)];
                double price = 10 + rng.nextInt(490);
                double rating = Math.round((1.0 + rng.nextDouble() * 4.0) * 100) / 100.0;
                pp.setString(1, "Product-" + i);
                pp.setString(2, cat);
                pp.setBigDecimal(3, BigDecimal.valueOf(price));
                pp.setInt(4, 50 + rng.nextInt(200));
                pp.setBigDecimal(5, BigDecimal.valueOf(rating));
                pp.addBatch();
            }
            pp.executeBatch();
            conn.commit();
        }

        conn.setAutoCommit(true);
        LOG.info("[Simulator] Seed data insertada: 200 customers, 500 products.");
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : def;
    }
}
