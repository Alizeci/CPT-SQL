package escuelaing.edu.co.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.infrastructure.analysis.QueryRegistryLoader;
import escuelaing.edu.co.infrastructure.capture.CaptureToggle;
import escuelaing.edu.co.infrastructure.capture.JdbcWrapper;
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
 * Simulates e-commerce traffic against the demo database to produce
 * {@code load-profile.json} for phase 3.
 *
 * <p>In a real production application this simulator is not needed — real user
 * traffic flows through {@link JdbcWrapper} automatically. This class exists
 * only because the demo has no real users; it stands in for them by calling
 * the five instrumented queries in proportions that reflect the declared
 * traffic distribution in {@link ProductRepository}.</p>
 *
 * <h3>Usage</h3>
 * <pre>
 * ./gradlew :sql-testing-demo:runSimulator
 * </pre>
 *
 * <h3>Environment variables</h3>
 * <pre>
 * DB_URL                   (default: jdbc:postgresql://localhost:5432/ecommerce_demo)
 * DB_USER                  (default: demo)
 * DB_PASSWORD              (default: demo)
 * SIMULATION_SECS          (default: 60)
 * SIMULATOR_STMT_TIMEOUT_MS (default: 8000)
 * </pre>
 */
public class EcommerceSimulator {

    private static final Logger LOG = Logger.getLogger(EcommerceSimulator.class.getName());

    private static final int SIMULATION_SECS =
            Integer.parseInt(System.getenv().getOrDefault("SIMULATION_SECS", "60"));
    private static final int THINK_TIME_MS = 100;

    private static final String[] CATEGORIES =
            {"electronics", "clothing", "books", "sports", "home"};

    public static void main(String[] args) throws Exception {
        String url  = env("DB_URL",      "jdbc:postgresql://localhost:5432/ecommerce_demo");
        String user = env("DB_USER",     "demo");
        String pass = env("DB_PASSWORD", "demo");

        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        QueryRegistryLoader registry = new QueryRegistryLoader(mapper);
        registry.load();

        CaptureToggle toggle = new CaptureToggle();
        MetricsBuffer buffer = new MetricsBuffer();
        buffer.start();

        SamplingFilter filter = new SamplingFilter(registry);
        EcommerceSanitizationStrategy sanitization = new EcommerceSanitizationStrategy();
        JdbcWrapper wrapper = new JdbcWrapper(filter, buffer, toggle, sanitization);

        LOG.info("[Simulator] Connecting to " + url);
        try (Connection raw = DriverManager.getConnection(url, user, pass)) {
            applySchema(raw);
            insertSeedData(raw);

            int stmtTimeoutMs = Integer.parseInt(
                    System.getenv().getOrDefault("SIMULATOR_STMT_TIMEOUT_MS", "8000"));
            try (Statement st = raw.createStatement()) {
                st.execute("SET statement_timeout = '" + stmtTimeoutMs + "'");
            }
            LOG.info("[Simulator] statement_timeout=" + stmtTimeoutMs + "ms set.");

            Connection conn = wrapper.wrap(raw);
            EcommerceRepository repo = new EcommerceRepository(conn);
            Random rng = new Random(42);

            LOG.info("[Simulator] Simulating traffic for " + SIMULATION_SECS + " s...");
            long endMs = System.currentTimeMillis() + (long) SIMULATION_SECS * 1_000;

            while (System.currentTimeMillis() < endMs) {
                int    productId  = rng.nextInt(500) + 1;
                int    customerId = rng.nextInt(200) + 1;
                String category   = CATEGORIES[rng.nextInt(CATEGORIES.length)];

                try { repo.searchByCategory(category); }
                catch (SQLException e) {
                    LOG.warning("[Simulator] searchByCategory cancelled (" + e.getMessage() + ")");
                }

                try { repo.getProductDetail(productId); }
                catch (SQLException e) {
                    LOG.fine("[Simulator] getProductDetail: " + e.getMessage());
                }

                if (rng.nextInt(5) == 0) {
                    try { repo.checkInventory(productId); }
                    catch (SQLException e) {
                        LOG.fine("[Simulator] checkInventory: " + e.getMessage());
                    }
                }

                if (rng.nextInt(10) == 0) {
                    try {
                        int orderId = repo.createOrder(customerId);
                        if (orderId > 0) repo.updateInventory(productId, -1);
                    } catch (SQLException e) {
                        LOG.fine("[Simulator] createOrder/updateInventory: " + e.getMessage());
                    }
                }

                Thread.sleep(THINK_TIME_MS);
            }
        }

        buffer.stop();
        LoadProfileBuilder builder = new LoadProfileBuilder(buffer);
        LoadProfile profile = builder.build();

        Path out = Path.of("load-profile.json");
        mapper.writerWithDefaultPrettyPrinter().writeValue(out.toFile(), profile);

        LOG.info("[Simulator] load-profile.json saved — "
                + profile.getTotalSamples() + " samples, "
                + profile.getQueries().size() + " queries captured.");
    }

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
        LOG.info("[Simulator] Schema applied.");
    }

    private static void insertSeedData(Connection conn) throws SQLException {
        try (PreparedStatement chk = conn.prepareStatement("SELECT COUNT(*) FROM products");
             ResultSet rs = chk.executeQuery()) {
            if (rs.next() && rs.getLong(1) > 0) {
                LOG.info("[Simulator] Seed data already exists — skipping.");
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

            for (int i = 1; i <= 5000; i++) {
                pc.setString(1, "Customer " + i);
                pc.setString(2, "c" + i + "@demo.test");
                pc.setString(3, "STANDARD");
                pc.addBatch();
            }
            pc.executeBatch();

            for (int i = 1; i <= 5000; i++) {
                String cat    = CATEGORIES[rng.nextInt(CATEGORIES.length)];
                double price  = 10 + rng.nextInt(490);
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
        LOG.info("[Simulator] Seed data inserted: 5000 customers, 5000 products.");
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : def;
    }
}
