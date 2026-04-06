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
 * <p>La query de {@link #searchByCategory} está paginada y ordenada por rating,
 * apoyándose en {@code idx_products_active_category}. Una feature aparentemente
 * razonable — ordenar por popularidad agregando un {@code LEFT JOIN order_items}
 * con {@code GROUP BY} — no tiene índice de soporte y degrada el p95 por encima
 * del SLA de 300 ms con datos a escala de producción. El problema no es visible
 * en entornos de desarrollo con pocos datos.</p>
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
     * Busca productos activos por categoría con paginación explícita.
     *
     * <p><b>Baseline:</b> paginación estándar ordenada por rating, apoyada en
     * {@code idx_products_active_category}. p95 esperado: &lt; 50 ms.</p>
     *
     * <p><b>Degradación típica:</b> agregar un {@code LEFT JOIN order_items}
     * con {@code GROUP BY} para ordenar por popularidad fuerza un hash aggregate
     * sobre millones de filas sin índice de soporte — invisible en dev,
     * crítico en producción.</p>
     */
    public List<String> searchByCategory(String category) throws SQLException {
        try (CaptureContext ignored = CaptureContext.begin("searchProductsByCategory");
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT p.id, p.name, p.price, p.stock_quantity, p.rating, " +
                     "       (SELECT COALESCE(SUM(oi.quantity), 0) " +
                     "        FROM order_items oi " +
                     "        WHERE oi.product_id = p.id) AS total_sold " +
                     "FROM products p " +
                     "WHERE p.active = true AND p.category = ? " +
                     "ORDER BY total_sold DESC " +
                     "LIMIT 20 OFFSET 0")) {
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
                     "WHERE id = ? AND stock_quantity + ? >= 0")) {
            ps.setInt(1, delta);
            ps.setInt(2, productId);
            ps.setInt(3, delta);
            ps.executeUpdate();
        }
    }
}
