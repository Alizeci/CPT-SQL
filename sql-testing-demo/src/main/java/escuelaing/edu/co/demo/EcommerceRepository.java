package escuelaing.edu.co.demo;

import escuelaing.edu.co.infrastructure.capture.CaptureContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Repositorio JDBC puro de la aplicación demo e-commerce.
 *
 * <p>No usa Spring ni ORM — solo {@link PreparedStatement} estándar.
 * Representa cómo cualquier equipo Java escribiría su capa de acceso a datos.</p>
 *
 * <p>Cada método abre un {@link CaptureContext} con el {@code queryId} declarado
 * en {@link ProductRepository} (Fase 1). Esto permite que el {@code JdbcWrapper}
 * asocie la latencia medida al identificador correcto sin modificar la firma
 * JDBC ni agregar dependencias al código de negocio.</p>
 *
 * <h3>Escenario de degradación demostrable</h3>
 * <p>Cambiar {@link #searchByCategory} de un SELECT simple a una subquery
 * correlacionada (por ejemplo, agregar conteo de órdenes por producto) degrada
 * el p95 por encima del SLA de 100 ms bajo carga Zipf. El {@code DegradationDetector}
 * reporta {@code P95_EXCEEDED} y el pipeline de CI falla.</p>
 */
public class EcommerceRepository {

    private final Connection conn;

    public EcommerceRepository(Connection conn) {
        this.conn = conn;
    }

    // -------------------------------------------------------------------------
    // Consultas de catálogo
    // -------------------------------------------------------------------------

    /**
     * Busca productos activos por categoría.
     *
     * <p><b>Baseline (fast):</b> SELECT simple con índice
     * {@code idx_products_active_category}. p95 esperado: ~10 ms.</p>
     *
     * <p><b>Degradación típica:</b> agregar una subquery correlacionada
     * {@code (SELECT COUNT(*) FROM order_items WHERE product_id = p.id)}
     * dispara el p95 por encima de 100 ms bajo carga Zipf.</p>
     */
    public List<String> searchByCategory(String category) throws SQLException {
        try (CaptureContext ignored = CaptureContext.begin("searchProductsByCategory");
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT p.id, p.name, p.price, p.stock_quantity, p.rating, " +
                     "       (SELECT COUNT(*) FROM order_items oi WHERE oi.product_id = p.id) AS order_count " +
                     "FROM products p " +
                     "WHERE p.active = true AND LOWER(p.category) = LOWER(?) " +
                     "ORDER BY order_count DESC, p.rating DESC")) {
            ps.setString(1, category);
            try (ResultSet rs = ps.executeQuery()) {
                List<String> names = new ArrayList<>();
                while (rs.next()) names.add(rs.getString("name"));
                return names;
            }
        }
    }

    /**
     * Obtiene el detalle de un producto por ID (PK lookup).
     */
    public void getProductDetail(int productId) throws SQLException {
        try (CaptureContext ignored = CaptureContext.begin("getProductDetail");
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT id, name, category, price, stock_quantity, rating, active " +
                     "FROM products " +
                     "WHERE id = ?")) {
            ps.setInt(1, productId);
            try (ResultSet rs = ps.executeQuery()) { rs.next(); }
        }
    }

    /**
     * Verifica el stock disponible de un producto antes del checkout.
     */
    public void checkInventory(int productId) throws SQLException {
        try (CaptureContext ignored = CaptureContext.begin("checkInventory");
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT stock_quantity FROM products WHERE id = ?")) {
            ps.setInt(1, productId);
            try (ResultSet rs = ps.executeQuery()) { rs.next(); }
        }
    }

    // -------------------------------------------------------------------------
    // Operaciones de escritura
    // -------------------------------------------------------------------------

    /**
     * Crea una nueva orden para el cliente dado.
     *
     * @return ID de la orden creada, o {@code -1} si falla
     */
    public int createOrder(int customerId) throws SQLException {
        try (CaptureContext ignored = CaptureContext.begin("createOrder");
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO orders(customer_id, status, total_amount) " +
                     "VALUES (?, 'PENDING', 0) RETURNING id")) {
            ps.setInt(1, customerId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : -1;
            }
        }
    }

    /**
     * Descuenta unidades del stock de un producto (venta confirmada).
     *
     * @param productId producto a actualizar
     * @param delta     cantidad a descontar (negativo = salida)
     */
    public void updateInventory(int productId, int delta) throws SQLException {
        try (CaptureContext ignored = CaptureContext.begin("updateInventory");
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE products " +
                     "SET stock_quantity = stock_quantity + ? " +
                     "WHERE id = ?")) {
            ps.setInt(1, delta);
            ps.setInt(2, productId);
            ps.executeUpdate();
        }
    }
}
