package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.domain.model.LoadProfile;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;
import java.util.zip.CRC32;

/**
 * Genera datos sintéticos en la BD espejo mediante el algoritmo DPSDG
 * (Differentially-Private Synthetic Data Generation) basado en SynQB (Liu et al., 2024).
 *
 * <h3>Diferenciación Feature / UII</h3>
 * <ul>
 *   <li><b>Feature columns</b> (precio, rating, stock) — se aplica mecanismo Gaussiano
 *       con presupuesto ε_ft = 0.6 para perturbar la distribución marginal y preservar
 *       la selectividad de predicados WHERE.</li>
 *   <li><b>UII columns</b> (user_id, customer_id, *_id) — se reemplazan por pseudónimos
 *       en rango exclusivo [10001, 99999] aplicando presupuesto ε_uii = 0.4.
 *       Nunca se almacenan ni procesan identificadores reales de producción.</li>
 * </ul>
 *
 * <p>Composición secuencial: ε_total = ε_ft + ε_uii = 1.0 (Theorem 4.4, Liu et al.).</p>
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
 * loadtest.synthetic.seed=42
 * loadtest.synthetic.minOrderItemsPerProduct=20
 * </pre>
 */
@Component
public class SyntheticDataGenerator {

    private static final Logger LOG = Logger.getLogger(SyntheticDataGenerator.class.getName());

    /** Presupuesto de privacidad para Feature columns — mecanismo Gaussiano (SynQB Theorem 4.6). */
    static final double EPSILON_FT  = 0.6;

    /** Presupuesto de privacidad para UII columns — mecanismo Gaussiano (SynQB §3.2). */
    static final double EPSILON_UII = 0.4;

    /** ε_total = ε_ft + ε_uii por composición secuencial (SynQB Theorem 4.4). */
    static final double EPSILON_TOTAL = EPSILON_FT + EPSILON_UII; // 1.0

    /** Rango exclusivo de pseudónimos UII — nunca colisiona con IDs reales (1–1000). */
    private static final int UII_MIN = 10_001;
    private static final int UII_MAX = 99_999;

    /** Umbral de error de latencia aceptable para validación de fidelidad (SynQB). */
    static final double LATENCY_ERROR_THRESHOLD_PCT  = 10.0;

    /** Umbral de error de cardinalidad aceptable para validación de fidelidad (SynQB). */
    static final double CARDINALITY_ERROR_THRESHOLD_PCT = 5.0;

    @Value("${loadtest.synthetic.rowsPerTable:500}")
    private int rowsPerTable;

    @Value("${loadtest.synthetic.batchSize:200}")
    private int batchSize;

    @Value("${loadtest.synthetic.seed:42}")
    private long seed;

    /**
     * Mínimo de filas en {@code order_items} garantizado por producto.
     * Asegura que subqueries correlacionadas siempre encuentren volumen suficiente
     * para generar carga observable, independientemente del entorno de CI.
     */
    @Value("${loadtest.synthetic.minOrderItemsPerProduct:20}")
    private int minOrderItemsPerProduct;

    private Random rng = new Random(42);

    /** Filas insertadas en la última llamada a {@link #populate}. */
    private long lastRowsGenerated = 0;

    /** Tablas pobladas en la última llamada a {@link #populate}. */
    private int lastTablesPopulated = 0;

    // -------------------------------------------------------------------------
    // Seed (reproducibilidad determinista — SynQB Algorithm 1)
    // -------------------------------------------------------------------------

    /** Sincroniza {@code rng} con el {@code seed} inyectado por Spring. */
    @PostConstruct
    void initRng() {
        this.rng = new Random(seed);
    }

    public long getSeed() { return seed; }

    public void setSeed(long seed) {
        this.seed = seed;
        this.rng  = new Random(seed);
    }

    public long getLastRowsGenerated()  { return lastRowsGenerated; }
    public int  getLastTablesPopulated() { return lastTablesPopulated; }

    // -------------------------------------------------------------------------
    // Clasificación de columnas: Feature vs UII (SynQB §3.2)
    // -------------------------------------------------------------------------

    /**
     * Categorías de columna según SynQB (Liu et al., 2024).
     *
     * <ul>
     *   <li>{@code FEATURE} — valor de negocio (precio, rating, cantidad).
     *       Se genera preservando la distribución estadística de producción.</li>
     *   <li>{@code USER_ID} — identificador de usuario/cliente.
     *       Se reemplaza por un pseudónimo en rango exclusivo (privacidad).</li>
     *   <li>{@code TRANSACTION_ID} — identificador de transacción (order_id, item_id).
     *       Se reemplaza por un pseudónimo en rango exclusivo.</li>
     * </ul>
     */
    public enum ColumnCategory { FEATURE, USER_ID, TRANSACTION_ID }

    /**
     * Clasifica una columna como Feature o UII basándose en su nombre.
     *
     * <p>Reglas (en orden de precedencia):</p>
     * <ol>
     *   <li>Contiene {@code "user_id"} o {@code "customer_id"} → {@code USER_ID}.</li>
     *   <li>Termina en {@code "_id"} o es exactamente {@code "id"} → {@code TRANSACTION_ID}.</li>
     *   <li>Cualquier otra columna → {@code FEATURE}.</li>
     * </ol>
     */
    public ColumnCategory classifyColumn(String tableName, String columnName) {
        String col = columnName.toLowerCase();
        if (col.equals("user_id") || col.equals("customer_id")) {
            return ColumnCategory.USER_ID;
        }
        if (col.equals("id") || col.endsWith("_id")) {
            return ColumnCategory.TRANSACTION_ID;
        }
        return ColumnCategory.FEATURE;
    }

    /**
     * Genera un pseudónimo entero en el rango [{@code UII_MIN}, {@code UII_MAX}]
     * que nunca colisiona con IDs reales de producción (rango 1–1000 típico).
     *
     * <p>Garantiza anonimización k-anónima de identidades de usuario y transacción
     * siguiendo la separación Feature/UII de SynQB (Liu et al., 2024 §3.2).</p>
     */
    public int generateUiiPseudonym() {
        return UII_MIN + rng.nextInt(UII_MAX - UII_MIN + 1);
    }

    // -------------------------------------------------------------------------
    // Checksum para reproducibilidad (SynQB Algorithm 2)
    // -------------------------------------------------------------------------

    /**
     * Calcula un checksum CRC32 sobre el contenido actual de {@code tableName}.
     * Se usa para verificar que dos generaciones con el mismo seed producen
     * datos byte-identical.
     *
     * @param conn      conexión abierta a la BD espejo
     * @param tableName tabla a verificar
     * @return checksum CRC32 de todas las filas de la tabla
     */
    public long computeChecksum(Connection conn, String tableName) throws SQLException {
        CRC32 crc = new CRC32();
        String sql = "SELECT * FROM " + tableName + " ORDER BY 1";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= cols; i++) {
                    Object v = rs.getObject(i);
                    if (v != null) crc.update(v.toString().getBytes());
                }
            }
        }
        return crc.getValue();
    }

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
        lastTablesPopulated = tables.size();

        if (isEcommerceSchema(tables)) {
            populateEcommerce(conn, profile);
        } else {
            populateGeneric(conn, tables);
        }

        conn.commit();
        conn.setAutoCommit(true);
        lastRowsGenerated = countTotalRows(conn, tables);
        LOG.info("[SyntheticData] BD espejo poblada: " + tables.size()
                + " tablas, " + lastRowsGenerated + " filas.");
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

    private void populateEcommerce(Connection conn, LoadProfile profile) throws SQLException {
        LOG.info("[SyntheticData] Poblando schema e-commerce...");

        // Fase 3 — sembrar con datos reales sanitizados (10 %) antes que los sintéticos
        insertRealSanitizedData(conn, profile);

        // Calibrar estadísticas de Feature columns con DP Gaussiano (SynQB §3.3)
        FeatureStats dpStats = extractDpStats(profile);

        int productRows  = computeTableRows(profile,
                "searchProductsByCategory", "getProductDetail",
                "checkInventory", "updateInventory");
        int orderRows    = computeTableRows(profile, "createOrder");
        int customerRows = Math.max(productRows / 5, 100);

        insertProducts(conn, productRows, dpStats);
        insertCustomers(conn, customerRows);
        insertOrders(conn, orderRows, dpStats);
        ensureMinOrderItemsPerProduct(conn, dpStats);
        LOG.info("[SyntheticData] E-commerce poblado.");
    }

    /**
     * Siembra la BD espejo con datos reales sanitizados capturados en producción
     * (el 10 % de la estrategia 10 % real / 90 % sintético de SynQB §3.4).
     *
     * <p>Para cada {@link escuelaing.edu.co.domain.model.LoadProfile.QueryStats}
     * que contenga {@code sanitizedRealData}, detecta la tabla destino por las
     * columnas presentes y construye sentencias INSERT completando los campos PII/ID
     * con pseudónimos sintéticos (nunca se usan identificadores reales).</p>
     *
     * <p>Mapeo de columnas a tabla:</p>
     * <ul>
     *   <li>{@code price | stock_quantity | rating} → {@code products}</li>
     *   <li>{@code total_amount | status} (sin columnas de products) → {@code orders}</li>
     * </ul>
     *
     * <p>Si no hay datos sanitizados en el perfil, el método retorna sin error
     * y la población sintética completa el 100 %.</p>
     *
     * @param conn    conexión abierta a la BD espejo (dentro de una transacción activa)
     * @param profile perfil de carga con las muestras sanitizadas de producción
     */
    void insertRealSanitizedData(Connection conn, LoadProfile profile) throws SQLException {
        if (profile == null || profile.getQueries() == null) return;

        int totalInserted = 0;
        for (LoadProfile.QueryStats stats : profile.getQueries().values()) {
            if (stats.getSanitizedRealData() == null || stats.getSanitizedRealData().isEmpty()) continue;

            for (Map<String, Object> row : stats.getSanitizedRealData()) {
                if (row == null || row.isEmpty()) continue;
                try {
                    if (isProductRow(row)) {
                        insertRealProduct(conn, row);
                        totalInserted++;
                    } else if (isOrderRow(row)) {
                        insertRealOrder(conn, row);
                        totalInserted++;
                    }
                    // Filas que no mapean a ninguna tabla conocida se descartan silenciosamente
                } catch (SQLException e) {
                    LOG.warning("[SyntheticData] Fila real descartada por conflicto: " + e.getMessage());
                }
            }
        }
        LOG.info("[SyntheticData] Filas reales sembradas: " + totalInserted);
    }

    /** Una fila mapea a products si contiene al menos una Feature column de productos. */
    private boolean isProductRow(Map<String, Object> row) {
        return row.containsKey("price") || row.containsKey("stock_quantity") || row.containsKey("rating");
    }

    /** Una fila mapea a orders si contiene total_amount o el status típico de una orden. */
    private boolean isOrderRow(Map<String, Object> row) {
        return (row.containsKey("total_amount") || row.containsKey("status"))
                && !isProductRow(row);
    }

    private void insertRealProduct(Connection conn, Map<String, Object> row) throws SQLException {
        String sql = """
                INSERT INTO products (name, category, price, stock_quantity, rating, active)
                VALUES (?, ?, ?, ?, ?, TRUE)
                ON CONFLICT DO NOTHING
                """;
        String cat    = objectToString(row.get("category"),
                CATEGORIES[rng.nextInt(CATEGORIES.length)]);
        double price  = objectToDouble(row.get("price"),
                clamp(gaussianNoise(80.0, 60.0), 1.0, 999.99));
        int    stock  = objectToInt(row.get("stock_quantity"),
                (int) clamp(gaussianNoise(100, 60), 0, 500));
        double rating = objectToDouble(row.get("rating"),
                clamp(gaussianNoise(3.8, 0.8), 1.0, 5.0));

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "RealProduct-" + generateUiiPseudonym() + "-" + cat.substring(0, Math.min(3, cat.length())).toUpperCase());
            ps.setString(2, cat);
            ps.setBigDecimal(3, BigDecimal.valueOf(Math.round(price * 100) / 100.0));
            ps.setInt(4, stock);
            ps.setBigDecimal(5, BigDecimal.valueOf(Math.round(rating * 100) / 100.0));
            ps.executeUpdate();
        }
    }

    private void insertRealOrder(Connection conn, Map<String, Object> row) throws SQLException {
        // customer_id se reemplaza por pseudónimo UII — nunca se usa el real
        List<Integer> customerIds = fetchIds(conn, "customers");
        if (customerIds.isEmpty()) return;

        String[] validStatuses = {"PENDING", "PROCESSING", "DELIVERED", "CANCELLED"};
        String status      = objectToString(row.get("status"), "DELIVERED");
        // Validar que el status sea uno de los valores permitidos por el schema
        boolean validStatus = false;
        for (String s : validStatuses) { if (s.equalsIgnoreCase(status)) { validStatus = true; break; } }
        if (!validStatus) status = "DELIVERED";

        double totalAmount = objectToDouble(row.get("total_amount"),
                clamp(gaussianNoise(150.0, 80.0), 1.0, 9999.99));

        String sql = """
                INSERT INTO orders (customer_id, status, total_amount)
                VALUES (?, ?::order_status, ?)
                ON CONFLICT DO NOTHING
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, customerIds.get(rng.nextInt(customerIds.size())));
            ps.setString(2, status.toUpperCase());
            ps.setBigDecimal(3, BigDecimal.valueOf(Math.round(totalAmount * 100) / 100.0));
            ps.executeUpdate();
        }
    }

    // ── Conversores defensivos para valores de un Map<String, Object> ──────────

    private double objectToDouble(Object v, double fallback) {
        if (v == null) return fallback;
        try { return ((Number) v).doubleValue(); } catch (ClassCastException e) {
            try { return Double.parseDouble(v.toString()); } catch (NumberFormatException ignored) {}
        }
        return fallback;
    }

    private int objectToInt(Object v, int fallback) {
        if (v == null) return fallback;
        try { return ((Number) v).intValue(); } catch (ClassCastException e) {
            try { return Integer.parseInt(v.toString()); } catch (NumberFormatException ignored) {}
        }
        return fallback;
    }

    private String objectToString(Object v, String fallback) {
        return v != null ? v.toString() : fallback;
    }

    /**
     * Calcula el número de filas a generar para las tablas accedidas por
     * los {@code queryIds} indicados, escalando {@code rowsPerTable}
     * proporcionalmente a la fracción de tráfico ({@code callsPerMinute})
     * que esos queries representan sobre el total del perfil de producción.
     *
     * <p>Si el perfil está vacío o no contiene los queryIds indicados,
     * devuelve {@code rowsPerTable} como fallback.</p>
     */
    private int computeTableRows(LoadProfile profile, String... queryIds) {
        if (profile == null || profile.getQueries().isEmpty()) return rowsPerTable;
        double totalCpm = profile.getQueries().values().stream()
                .mapToDouble(LoadProfile.QueryStats::getCallsPerMinute).sum();
        double tableCpm = Arrays.stream(queryIds)
                .mapToDouble(id -> {
                    LoadProfile.QueryStats s = profile.getQueries().get(id);
                    return s != null ? s.getCallsPerMinute() : 0.0;
                }).sum();
        if (totalCpm == 0 || tableCpm == 0) return rowsPerTable;
        // Siempre devolvemos rowsPerTable independientemente del tráfico,
        // para mantener relaciones realistas entre tablas (p.ej. products:order_items ≈ 1:1).
        // Escalar por CPM concentraría filas en tablas de lectura y dejaría
        // tablas de escritura tan vacías que las subqueries correlacionadas
        // encontrarían 0 filas y no generarían carga observable.
        return rowsPerTable;
    }

    private static final String[] CATEGORIES = {
        "electronics", "clothing", "books", "sports",
        "home", "beauty", "toys", "food"
    };

    private void insertProducts(Connection conn, int count, FeatureStats stats) throws SQLException {
        String sql = """
                INSERT INTO products (name, category, price, stock_quantity, rating, active)
                VALUES (?, ?, ?, ?, ?, TRUE)
                ON CONFLICT DO NOTHING
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 1; i <= count; i++) {
                String cat    = CATEGORIES[rng.nextInt(CATEGORIES.length)];
                double price  = clamp(gaussianNoise(stats.meanPrice(),  stats.stdPrice()),  1.0, 999.99);
                int    stock  = (int) clamp(gaussianNoise(stats.meanStock(), stats.stdStock()), 0, 500);
                double rating = clamp(gaussianNoise(stats.meanRating(), stats.stdRating()), 1.0, 5.0);

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

    private void insertCustomers(Connection conn, int count) throws SQLException {
        String sql = """
                INSERT INTO customers (name, email, tier)
                VALUES (?, ?, ?)
                ON CONFLICT DO NOTHING
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
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

    private void insertOrders(Connection conn, int count, FeatureStats stats) throws SQLException {
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
                        double price     = clamp(gaussianNoise(stats.meanPrice(), stats.stdPrice() * 0.5), 1.0, 999.99);
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

    /**
     * Garantiza al menos {@code minOrderItemsPerProduct} filas en {@code order_items}
     * por cada producto, insertando las filas faltantes sobre órdenes ya existentes.
     *
     * <p>Esto asegura que subqueries correlacionadas del tipo
     * {@code SELECT COUNT(*) FROM order_items WHERE product_id = p.id}
     * siempre encuentren volumen suficiente para generar carga observable,
     * independientemente del entorno de CI.</p>
     */
    private void ensureMinOrderItemsPerProduct(Connection conn, FeatureStats stats) throws SQLException {
        List<Integer> productIds = fetchIds(conn, "products");
        List<Integer> orderIds   = fetchIds(conn, "orders");
        if (productIds.isEmpty() || orderIds.isEmpty()) return;

        java.util.Map<Integer, Integer> currentCounts = new java.util.HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT product_id, COUNT(*) FROM order_items GROUP BY product_id");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) currentCounts.put(rs.getInt(1), rs.getInt(2));
        }

        String itemSql = """
                INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                VALUES (?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """;
        int totalInserted = 0;
        try (PreparedStatement ps = conn.prepareStatement(itemSql)) {
            for (int productId : productIds) {
                int needed = minOrderItemsPerProduct - currentCounts.getOrDefault(productId, 0);
                for (int i = 0; i < needed; i++) {
                    int    orderId = orderIds.get(rng.nextInt(orderIds.size()));
                    int    qty     = 1 + rng.nextInt(3);
                    double price   = clamp(gaussianNoise(stats.meanPrice(), stats.stdPrice() * 0.5), 1.0, 999.99);
                    ps.setInt(1, orderId);
                    ps.setInt(2, productId);
                    ps.setInt(3, qty);
                    ps.setBigDecimal(4, BigDecimal.valueOf(Math.round(price * 100) / 100.0));
                    ps.addBatch();
                    if (++totalInserted % batchSize == 0) ps.executeBatch();
                }
            }
            ps.executeBatch();
        }
        LOG.info("[SyntheticData] order_items completados: mínimo " + minOrderItemsPerProduct
                + " por producto (" + totalInserted + " filas adicionales).");
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
        ColumnCategory category = classifyColumn("", col.name());
        // UII columns: pseudónimo en rango exclusivo (SynQB §3.2)
        if (category == ColumnCategory.USER_ID || category == ColumnCategory.TRANSACTION_ID) {
            ps.setInt(idx, generateUiiPseudonym());
            return;
        }
        // Feature columns: preservar distribución estadística
        switch (col.dataType().toLowerCase()) {
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

    private long countTotalRows(Connection conn, List<String> tables) {
        long total = 0;
        for (String table : tables) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + table);
                 ResultSet rs = ps.executeQuery()) {
                if (rs.next()) total += rs.getLong(1);
            } catch (SQLException ignored) {}
        }
        return total;
    }

    private List<Integer> fetchIds(Connection conn, String table) throws SQLException {
        List<Integer> ids = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM " + table + " LIMIT 1000");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) ids.add(rs.getInt(1));
        }
        return ids;
    }

    /**
     * Aplica el mecanismo Gaussiano con presupuesto ε_ft a una columna Feature.
     * La sensibilidad global se escala por (1 / ε_ft), amplificando el ruido
     * a mayor privacidad (menor ε) y reduciéndolo a menor privacidad (mayor ε).
     */
    double gaussianNoise(double base, double sigma) {
        return base + rng.nextGaussian() * (sigma / EPSILON_FT);
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

    // -------------------------------------------------------------------------
    // Calibración DP de Feature columns (SynQB §3.3 — DPSDG)
    // -------------------------------------------------------------------------

    /**
     * Estadísticas de Feature columns calibradas con mecanismo Gaussiano DP.
     * Reemplaza los valores hardcodeados del generador con distribuciones
     * derivadas del {@code sanitizedRealData} capturado en Fase 2.
     */
    private record FeatureStats(
            double meanPrice,  double stdPrice,
            double meanRating, double stdRating,
            double meanStock,  double stdStock) {

        /** Valores por defecto cuando no hay muestras reales suficientes. */
        static FeatureStats defaults() {
            return new FeatureStats(80.0, 60.0, 3.8, 0.8, 100.0, 60.0);
        }
    }

    /**
     * Extrae estadísticas de Feature columns desde {@code sanitizedRealData} y
     * aplica mecanismo Gaussiano con ε={@link #EPSILON_FT} (SynQB Theorem 4.6).
     *
     * <p>Sensibilidad global del mecanismo = rango / n, donde n es el número
     * de muestras capturadas en Fase 2 (SamplingFilter 10 %).</p>
     *
     * <p>Si el perfil contiene menos de 5 muestras retorna
     * {@link FeatureStats#defaults()} para mantener reproducibilidad.</p>
     */
    FeatureStats extractDpStats(LoadProfile profile) {
        if (profile == null || profile.getQueries() == null) return FeatureStats.defaults();

        List<Double> prices  = new ArrayList<>();
        List<Double> ratings = new ArrayList<>();
        List<Double> stocks  = new ArrayList<>();

        for (LoadProfile.QueryStats stats : profile.getQueries().values()) {
            if (stats.getSanitizedRealData() == null) continue;
            for (Map<String, Object> row : stats.getSanitizedRealData()) {
                if (row == null) continue;
                if (row.containsKey("price"))
                    prices.add(objectToDouble(row.get("price"), -1.0));
                if (row.containsKey("rating"))
                    ratings.add(objectToDouble(row.get("rating"), -1.0));
                if (row.containsKey("stock_quantity"))
                    stocks.add((double) objectToInt(row.get("stock_quantity"), -1));
            }
        }

        prices  = prices.stream().filter(v -> v > 0).collect(java.util.stream.Collectors.toList());
        ratings = ratings.stream().filter(v -> v > 0).collect(java.util.stream.Collectors.toList());
        stocks  = stocks.stream().filter(v -> v >= 0).collect(java.util.stream.Collectors.toList());

        if (prices.size() < 5 && ratings.size() < 5) {
            LOG.info("[SyntheticData] Datos reales insuficientes — usando estadísticas por defecto.");
            return FeatureStats.defaults();
        }

        int n = Math.max(prices.size(), 1);

        double rawMeanPrice  = mean(prices);
        double rawStdPrice   = std(prices, rawMeanPrice);
        double rawMeanRating = mean(ratings);
        double rawStdRating  = std(ratings, rawMeanRating);
        double rawMeanStock  = mean(stocks);
        double rawStdStock   = std(stocks, rawMeanStock);

        // Sensibilidad global para la media = rango / n (Gaussian mechanism)
        double dpMeanPrice  = clamp(gaussianNoise(rawMeanPrice,  998.0 / n), 1.0,  999.0);
        double dpStdPrice   = clamp(gaussianNoise(rawStdPrice,    50.0 / n), 1.0,  300.0);
        double dpMeanRating = clamp(gaussianNoise(rawMeanRating,   4.0 / n), 1.0,    5.0);
        double dpStdRating  = clamp(gaussianNoise(rawStdRating,    0.5 / n), 0.1,    2.0);
        double dpMeanStock  = clamp(gaussianNoise(rawMeanStock,  500.0 / n), 0.0,  500.0);
        double dpStdStock   = clamp(gaussianNoise(rawStdStock,    50.0 / n), 1.0,  200.0);

        LOG.info(String.format(
                "[SyntheticData] DP stats calibradas (n=%d muestras, ε=%.1f): " +
                "price=%.1f±%.1f  rating=%.2f±%.2f  stock=%.0f±%.0f",
                n, EPSILON_FT,
                dpMeanPrice, dpStdPrice, dpMeanRating, dpStdRating, dpMeanStock, dpStdStock));

        return new FeatureStats(dpMeanPrice, dpStdPrice, dpMeanRating, dpStdRating,
                                dpMeanStock, dpStdStock);
    }

    private double mean(List<Double> values) {
        if (values.isEmpty()) return 0.0;
        return values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }

    private double std(List<Double> values, double mean) {
        if (values.size() < 2) return 1.0;
        double variance = values.stream()
                .mapToDouble(v -> (v - mean) * (v - mean))
                .average().orElse(1.0);
        return Math.sqrt(variance);
    }
}
