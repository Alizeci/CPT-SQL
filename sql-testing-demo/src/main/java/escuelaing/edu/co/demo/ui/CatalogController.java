package escuelaing.edu.co.demo.ui;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Expone el catálogo de productos de la app demo e-commerce.
 *
 * <p>Usa intencionalmente la query degradada (3 subqueries correlacionadas)
 * para simular la pantalla lenta que llegó a producción antes de que
 * existiera el pipeline de pruebas de carga CPT-SQL.</p>
 */
@RestController
public class CatalogController {

    private static final String SQL_DEGRADED =
            "SELECT p.id, p.name, p.price, p.stock_quantity, p.rating, " +
            "  (SELECT COUNT(*) FROM order_items oi WHERE oi.product_id = p.id) AS total_orders, " +
            "  (SELECT ROUND(AVG(oi2.quantity)::numeric, 1) FROM order_items oi2 WHERE oi2.product_id = p.id) AS avg_qty, " +
            "  (SELECT MAX(o.created_at) FROM orders o " +
            "   JOIN order_items oi3 ON oi3.order_id = o.id " +
            "   WHERE oi3.product_id = p.id) AS last_order " +
            "FROM products p " +
            "WHERE active = true AND category = ? " +
            "ORDER BY total_orders DESC " +
            "LIMIT 50";

    private final JdbcTemplate jdbc;

    public CatalogController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/api/products")
    public Map<String, Object> getProducts(
            @RequestParam(defaultValue = "electronics") String category) {

        long start = System.currentTimeMillis();
        List<Map<String, Object>> products = jdbc.queryForList(SQL_DEGRADED, category);
        long queryTimeMs = System.currentTimeMillis() - start;

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("queryTimeMs", queryTimeMs);
        response.put("category", category);
        response.put("count", products.size());
        response.put("products", products);
        return response;
    }
}
