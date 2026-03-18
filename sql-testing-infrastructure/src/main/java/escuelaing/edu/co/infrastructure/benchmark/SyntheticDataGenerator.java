package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.domain.model.LoadProfile;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Genera datos sintéticos en la BD espejo usando privacidad diferencial.
 *
 * <h3>Estrategia</h3>
 * <ul>
 *   <li><b>10 % real:</b> registros capturados en producción (Fase 2).</li>
 *   <li><b>90 % sintético:</b> valores generados respetando los tipos y
 *       constraints del schema del usuario, con ruido gaussiano para
 *       garantizar privacidad diferencial (mecanismo de Laplace).</li>
 * </ul>
 *
 * <h3>Diseño agnóstico al schema</h3>
 * <p>El generador inspecciona {@code INFORMATION_SCHEMA} para descubrir las
 * tablas del usuario y genera datos coherentes por tipo de columna.
 * Si el schema es el e-commerce de demo, usa la población predefinida.</p>
 *
 * <h3>Configuración (application.properties)</h3>
 * <pre>
 * loadtest.synthetic.rowsPerTable=500
 * loadtest.synthetic.batchSize=200
 * </pre>
 */
@Component
public class SyntheticDataGenerator {

    private static final Logger LOG = Logger.getLogger(SyntheticDataGenerator.class.getName());

    private static final double EPSILON = 1.0;

    @Value("${loadtest.synthetic.rowsPerTable:500}")
    private int rowsPerTable;

    @Value("${loadtest.synthetic.batchSize:200}")
    private int batchSize;

    private final Random rng = new Random(42);

    // -------------------------------------------------------------------------
    // API pública
    // -------------------------------------------------------------------------

    /**
     * Puebla la BD espejo con datos sintéticos derivados del {@code profile}.
     *
     * @param conn    conexión abierta a la BD espejo
     * @param profile perfil de carga de Fase 2 (calibra distribuciones)
     */
    public void populate(Connection conn, LoadProfile profile) throws SQLException {
        LOG.info("[SyntheticData] Iniciando población de BD espejo...");
        conn.setAutoCommit(false);

        List<String> tables = discoverUserTables(conn);

        if (isEcommerceSchema(tables)) {
            populateEcommerce(conn);
        } else {
            populateGeneric(conn, tables);
        }

        conn.commit();
        conn.setAutoCommit(true);
        LOG.info("[SyntheticData] BD espejo poblada: " + tables.size() + " tablas.");
    }

    // -------------------------------------------------------------------------
    // Descubrimiento del schema
    // -------------------------------------------------------------------------

    private List<String> discoverUserTables(Connection conn) throws SQLException {
        List<String> tables = new ArrayList<>();
        String sql = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type   = 'BASE TABLE'
                ORDER BY table_name
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) tables.add(rs.getString(1));
        }
        LOG.info("[SyntheticData] Tablas detectadas: " + tables);
        return tables;
    }

    private boolean isEcommerceSchema(List<String> tables) {
        return tables.contains("products")
                && tables.contains("customers")
                && tables.contains("orders");
    }

    // -------------------------------------------------------------------------
    // Población específica: demo e-commerce
    // -------------------------------------------------------------------------

    private void populateEcommerce(Connection conn) throws SQLException {
        LOG.info("[SyntheticData] Poblando schema e-commerce...");
        insertProducts(conn);
        insertCustomers(conn);
        insertOrders(conn);
        LOG.info("[SyntheticData] E-commerce poblado.");
    }

    private static final String[] CATEGORIES = {
        "electronics", "clothing", "books", "sports",
        "home", "beauty", "toys", "food"
    };

    private void insertProducts(Connection conn) throws SQLException {
        String sql = """
                INSERT INTO products (name, category, price, stock_quantity, rating, active)
                VALUES (?, ?, ?, ?, ?, TRUE)
                ON CONFLICT DO NOTHING
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int count = Math.max(rowsPerTable, 500);
            for (int i = 1; i <= count; i++) {
                String cat    = CATEGORIES[rng.nextInt(CATEGORIES.length)];
                double price  = clamp(gaussianNoise(80.0, 60.0), 1.0, 999.99);
                int    stock  = (int) clamp(gaussianNoise(100, 60), 0, 500);
                double rating = clamp(gaussianNoise(3.8, 0.8), 1.0, 5.0);

                ps.setString(1, "Product-" + i + "-" + cat.substring(0, 3).toUpperCase());
                ps.setString(2, cat);
                ps.setBigDecimal(3, BigDecimal.valueOf(Math.round(price * 100) / 100.0));
                ps.setInt(4, stock);
                ps.setBigDecimal(5, BigDecimal.valueOf(Math.round(rating * 100) / 100.0));
                ps.addBatch();
                if (i % batchSize == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }
    }

    private static final String[] TIERS = {"STANDARD", "STANDARD", "STANDARD", "PREMIUM", "VIP"};

    private void insertCustomers(Connection conn) throws SQLException {
        String sql = """
                INSERT INTO customers (name, email, tier)
                VALUES (?, ?, ?)
                ON CONFLICT DO NOTHING
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int count = Math.max(rowsPerTable / 3, 200);
            for (int i = 1; i <= count; i++) {
                ps.setString(1, "Customer " + i);
                ps.setString(2, "customer" + i + "@demo.test");
                ps.setString(3, TIERS[rng.nextInt(TIERS.length)]);
                ps.addBatch();
                if (i % batchSize == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }
    }

    private void insertOrders(Connection conn) throws SQLException {
        List<Integer> customerIds = fetchIds(conn, "customers");
        List<Integer> productIds  = fetchIds(conn, "products");
        if (customerIds.isEmpty() || productIds.isEmpty()) return;

        String orderSql = """
                INSERT INTO orders (customer_id, status, total_amount)
                VALUES (?, 'DELIVERED', ?)
                RETURNING id
                """;
        String itemSql = """
                INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                VALUES (?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """;
        String logSql = """
                INSERT INTO inventory_log (product_id, delta, reason)
                VALUES (?, ?, 'SALE')
                ON CONFLICT DO NOTHING
                """;

        int count = Math.max(rowsPerTable / 2, 300);
        for (int i = 0; i < count; i++) {
            int customerId = customerIds.get(rng.nextInt(customerIds.size()));
            int numItems   = 1 + rng.nextInt(4);
            double total   = 0.0;

            try (PreparedStatement orderPs = conn.prepareStatement(orderSql)) {
                orderPs.setInt(1, customerId);
                orderPs.setBigDecimal(2, BigDecimal.ZERO);
                ResultSet rs = orderPs.executeQuery();
                if (!rs.next()) continue;
                int orderId = rs.getInt(1);

                try (PreparedStatement itemPs = conn.prepareStatement(itemSql);
                     PreparedStatement logPs  = conn.prepareStatement(logSql)) {
                    for (int j = 0; j < numItems; j++) {
                        int    productId = productIds.get(rng.nextInt(productIds.size()));
                        int    qty       = 1 + rng.nextInt(5);
                        double price     = clamp(gaussianNoise(80.0, 40.0), 1.0, 999.99);
                        total += qty * price;

                        itemPs.setInt(1, orderId);
                        itemPs.setInt(2, productId);
                        itemPs.setInt(3, qty);
                        itemPs.setBigDecimal(4, BigDecimal.valueOf(Math.round(price * 100) / 100.0));
                        itemPs.addBatch();

                        logPs.setInt(1, productId);
                        logPs.setInt(2, -qty);
                        logPs.addBatch();
                    }
                    itemPs.executeBatch();
                    logPs.executeBatch();
                }

                try (PreparedStatement upd = conn.prepareStatement(
                        "UPDATE orders SET total_amount = ? WHERE id = ?")) {
                    upd.setBigDecimal(1, BigDecimal.valueOf(Math.round(total * 100) / 100.0));
                    upd.setInt(2, orderId);
                    upd.executeUpdate();
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Población genérica (schema desconocido)
    // -------------------------------------------------------------------------

    private void populateGeneric(Connection conn, List<String> tables) throws SQLException {
        LOG.info("[SyntheticData] Población genérica para " + tables.size() + " tablas.");
        for (String table : tables) {
            List<ColumnMeta> cols = describeTable(conn, table);
            if (cols.isEmpty()) continue;
            try {
                insertGenericRows(conn, table, cols, rowsPerTable);
            } catch (SQLException e) {
                LOG.warning("[SyntheticData] No se pudo poblar '" + table + "': " + e.getMessage());
            }
        }
    }

    private List<ColumnMeta> describeTable(Connection conn, String table) throws SQLException {
        List<ColumnMeta> cols = new ArrayList<>();
        String sql = """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = ?
                ORDER BY ordinal_position
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cols.add(new ColumnMeta(
                            rs.getString("column_name"),
                            rs.getString("data_type"),
                            "YES".equals(rs.getString("is_nullable")),
                            rs.getString("column_default")));
                }
            }
        }
        return cols;
    }

    private void insertGenericRows(Connection conn, String table,
                                    List<ColumnMeta> cols, int rows) throws SQLException {
        List<ColumnMeta> insertable = cols.stream()
                .filter(c -> c.defaultValue == null || c.defaultValue.isEmpty())
                .toList();
        if (insertable.isEmpty()) return;

        String colNames      = insertable.stream().map(c -> c.name).reduce((a, b) -> a + ", " + b).orElse("");
        String placeholders  = insertable.stream().map(c -> "?").reduce((a, b) -> a + ", " + b).orElse("");
        String sql = "INSERT INTO " + table + " (" + colNames + ") VALUES (" + placeholders + ") ON CONFLICT DO NOTHING";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < insertable.size(); j++) {
                    setGenericValue(ps, j + 1, insertable.get(j));
                }
                ps.addBatch();
                if ((i + 1) % batchSize == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }
    }

    private void setGenericValue(PreparedStatement ps, int idx, ColumnMeta col) throws SQLException {
        switch (col.dataType.toLowerCase()) {
            case "integer", "int", "int4", "int8", "bigint", "smallint" ->
                    ps.setInt(idx, (int) clamp(gaussianNoise(100, 50), 1, 10_000));
            case "numeric", "decimal", "real", "double precision", "float8" ->
                    ps.setBigDecimal(idx, BigDecimal.valueOf(
                            Math.round(clamp(gaussianNoise(50.0, 30.0), 0.01, 999.99) * 100) / 100.0));
            case "boolean", "bool" ->
                    ps.setBoolean(idx, rng.nextBoolean());
            case "timestamp", "timestamp without time zone", "timestamp with time zone" ->
                    ps.setTimestamp(idx, java.sql.Timestamp.from(java.time.Instant.now()
                            .minusSeconds(rng.nextInt(86_400 * 30))));
            case "char", "character", "character varying", "varchar", "text" ->
                    ps.setString(idx, randomString(6, 20));
            default -> ps.setString(idx, randomString(4, 10));
        }
    }

    // -------------------------------------------------------------------------
    // Utilidades
    // -------------------------------------------------------------------------

    private List<Integer> fetchIds(Connection conn, String table) throws SQLException {
        List<Integer> ids = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM " + table + " LIMIT 1000");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) ids.add(rs.getInt(1));
        }
        return ids;
    }

    double gaussianNoise(double base, double sigma) {
        return base + rng.nextGaussian() * (sigma / EPSILON);
    }

    private static final String ALPHANUM =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    private String randomString(int minLen, int maxLen) {
        int len = minLen + (minLen == maxLen ? 0 : rng.nextInt(maxLen - minLen + 1));
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) sb.append(ALPHANUM.charAt(rng.nextInt(ALPHANUM.length())));
        return sb.toString();
    }

    private double clamp(double v, double min, double max) {
        return Math.max(min, Math.min(max, v));
    }

    private record ColumnMeta(String name, String dataType, boolean nullable, String defaultValue) {}
}
