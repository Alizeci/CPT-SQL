package escuelaing.edu.co.demo;

import escuelaing.edu.co.processor.annotation.Req;
import escuelaing.edu.co.processor.annotation.SqlQuery;

/**
 * Repositorio de la aplicación demo E-commerce.
 *
 * <p>Instrumentado con {@link SqlQuery} y {@link Req} para demostrar el flujo
 * completo del sistema de pruebas de carga continua:</p>
 * <ol>
 *   <li>Fase 1 — el procesador de anotaciones extrae estos metadatos y genera
 *       {@code queries.json} en tiempo de compilación.</li>
 *   <li>Fase 2 — el {@code JdbcWrapper} captura latencias y SQL real en producción
 *       y construye el {@code LoadProfile}.</li>
 *   <li>Fase 3 — el {@code BenchmarkRunner} ejecuta estas consultas sobre la BD
 *       espejo (schema {@code schema-ecommerce.sql}) bajo el TestProfile "peak"
 *       (flash sale, distribución Zipf) y detecta degradaciones.</li>
 * </ol>
 *
 * <h3>Escenario de degradación demostrable</h3>
 * <p>Si un desarrollador agrega un {@code LEFT JOIN} sin índice a
 * {@code searchProductsByCategory}, el {@code DegradationDetector} detectará:</p>
 * <ul>
 *   <li>{@code PLAN_CHANGED} — el planner cambia de index scan a seq scan.</li>
 *   <li>{@code P95_EXCEEDED} — la latencia p95 supera el umbral de 100 ms
 *       durante la fase de pico (400 TPS, Zipf α=1.0).</li>
 * </ul>
 */
public class ProductRepository {

    // -------------------------------------------------------------------------
    // Consultas de catálogo (read-heavy — ~80 % del tráfico normal)
    // -------------------------------------------------------------------------

    /**
     * Busca productos activos por categoría.
     *
     * <p>La consulta más frecuente del sistema (~60 % del tráfico).
     * El índice {@code idx_products_active_category} es crítico: si se elimina
     * en un PR, esta consulta degradará de index scan a seq scan bajo carga Zipf
     * (hot spot en categorías populares como "electronics").</p>
     */
    @SqlQuery(queryId = "searchProductsByCategory",
              description = "Busca productos activos por categoría con paginación")
    @Req(maxResponseTimeMs = 100,
         priority = Req.Priority.HIGH,
         allowPlanChange = false,
         description = "SLA: 100 ms p95. Plan change prohibido — índice crítico para hot spots")
    public void searchProductsByCategory(String category, int limit, int offset) {
        // SELECT id, name, price, stock_quantity, rating
        // FROM products
        // WHERE active = true AND category = ?
        // ORDER BY rating DESC
        // LIMIT ? OFFSET ?
    }

    /**
     * Obtiene el detalle de un producto por ID.
     *
     * <p>Segunda consulta más frecuente (~20 % del tráfico).
     * Bajo distribución Zipf en el TestProfile "peak", los top-3 productos
     * de la flash sale concentran ~80 % de estas llamadas — hot spot directo.</p>
     */
    @SqlQuery(queryId = "getProductDetail",
              description = "Obtiene el detalle completo de un producto por ID")
    @Req(maxResponseTimeMs = 50,
         priority = Req.Priority.HIGH,
         allowPlanChange = false,
         description = "SLA: 50 ms p95. Acceso por PK — cualquier plan change es una degradación")
    public void getProductDetail(int productId) {
        // SELECT id, name, category, price, stock_quantity, rating, active
        // FROM products
        // WHERE id = ?
    }

    // -------------------------------------------------------------------------
    // Consultas de inventario (lectura crítica — ~10 % del tráfico normal)
    // -------------------------------------------------------------------------

    /**
     * Verifica el stock disponible de un producto.
     *
     * <p>Llamada antes de confirmar un carrito. En la fase de flash sale del
     * TestProfile "peak" (WRITE_HEAVY), el stock de los hot products llega a
     * cero y esta consulta genera contención con {@code updateInventory}.</p>
     */
    @SqlQuery(queryId = "checkInventory",
              description = "Verifica el stock disponible de un producto")
    @Req(maxResponseTimeMs = 30,
         priority = Req.Priority.HIGH,
         allowPlanChange = false,
         description = "SLA: 30 ms p95. Lectura crítica antes del checkout — plan change prohibido")
    public void checkInventory(int productId) {
        // SELECT stock_quantity FROM products WHERE id = ?
    }

    // -------------------------------------------------------------------------
    // Consultas de órdenes (write — ~10 % del tráfico normal → 60 % en pico)
    // -------------------------------------------------------------------------

    /**
     * Crea una nueva orden con sus líneas de detalle.
     *
     * <p>Transacción multi-tabla: INSERT en {@code orders} + N INSERTs en
     * {@code order_items} + UPDATE en {@code products.stock_quantity}.
     * Bajo la fase "flash_sale" del TestProfile "peak" (400 TPS, WRITE_HEAVY),
     * esta consulta genera la mayor contención en la BD espejo.</p>
     */
    @SqlQuery(queryId = "createOrder",
              description = "Crea una orden y sus líneas de detalle (multi-tabla)")
    @Req(maxResponseTimeMs = 200,
         priority = Req.Priority.HIGH,
         allowPlanChange = true,
         description = "SLA: 200 ms p95. Plan change permitido — write multi-tabla")
    public void createOrder(int customerId, int productId, int quantity) {
        // BEGIN
        //   INSERT INTO orders(customer_id, status, total_amount) VALUES (?, 'PENDING', ?)
        //   INSERT INTO order_items(order_id, product_id, quantity, unit_price) VALUES (?,?,?,?)
        //   UPDATE products SET stock_quantity = stock_quantity - ? WHERE id = ? AND stock_quantity >= ?
        //   INSERT INTO inventory_log(product_id, delta, reason) VALUES (?, ?, 'SALE')
        // COMMIT
    }

    /**
     * Actualiza el stock de un producto (descuento por venta).
     *
     * <p>Escritura concurrente: múltiples workers pueden actualizar el mismo
     * producto simultáneamente durante la flash sale — punto de contención máxima.</p>
     */
    @SqlQuery(queryId = "updateInventory",
              description = "Descuenta unidades del stock (venta confirmada)")
    @Req(maxResponseTimeMs = 100,
         priority = Req.Priority.HIGH,
         allowPlanChange = false,
         description = "SLA: 100 ms p95. Escritura concurrente crítica — plan change prohibido")
    public void updateInventory(int productId, int delta) {
        // UPDATE products
        // SET stock_quantity = stock_quantity + ?
        // WHERE id = ?
        //
        // INSERT INTO inventory_log(product_id, delta, reason)
        // VALUES (?, ?, 'SALE')
    }
}
