package escuelaing.edu.co.infrastructure.capture;

import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Estrategia de sanitización de datos capturados en Fase 2.
 *
 * <p>Aplica una política de <b>lista blanca</b>: solo las columnas explícitamente
 * permitidas se copian al registro. Cualquier columna que no esté en la lista
 * blanca se descarta, garantizando que datos personales (emails, nombres,
 * identificadores de usuario) nunca salgan del ambiente de producción.</p>
 *
 * <p>Cumple con la Ley 1581 de 2012 (Colombia) y principios GDPR: un dato
 * sanitizado es aquel del cual no puede derivarse la identidad de una persona
 * natural.</p>
 *
 * <h3>Columnas permitidas (lista blanca)</h3>
 * <ul>
 *   <li>{@code price} — precio del producto (Feature column)</li>
 *   <li>{@code category} — categoría del producto (Feature column)</li>
 *   <li>{@code quantity} — cantidad (Feature column)</li>
 *   <li>{@code stock_quantity} — stock disponible (Feature column)</li>
 *   <li>{@code status} — estado de la orden (Feature column)</li>
 *   <li>{@code payment_method} — método de pago (Feature column)</li>
 *   <li>{@code discount_percent} — porcentaje de descuento (Feature column)</li>
 *   <li>{@code rating} — calificación del producto (Feature column)</li>
 *   <li>{@code total_amount} — monto total de la orden (Feature column)</li>
 * </ul>
 *
 * <p>Columnas excluidas explícitamente (aunque estén en el ResultSet):
 * {@code user_id}, {@code customer_id}, {@code email}, {@code name},
 * {@code phone}, {@code address} y cualquier columna con sufijo {@code _id}
 * que no sea {@code product_id}.</p>
 */
@Component
public class SanitizationStrategy {

    /**
     * Columnas de negocio cuyo valor puede copiarse sin riesgo para la privacidad.
     * Corresponden a las Feature columns definidas en SynQB (Liu et al., 2024 §3.2).
     */
    static final Set<String> WHITELIST = Set.of(
            "price",
            "category",
            "quantity",
            "stock_quantity",
            "status",
            "payment_method",
            "discount_percent",
            "rating",
            "total_amount"
    );

    /**
     * Extrae del {@link ResultSet} solo las columnas permitidas por la lista blanca.
     *
     * <p>Si el {@code ResultSet} no tiene filas o no contiene ninguna columna
     * de la lista blanca, retorna un mapa vacío (nunca {@code null}).</p>
     *
     * @param rs ResultSet posicionado en la fila actual
     * @return mapa {@code nombreColumna → valor} con solo columnas sanitizadas
     * @throws SQLException si ocurre un error al leer el ResultSet
     */
    public Map<String, Object> sanitize(ResultSet rs) throws SQLException {
        Map<String, Object> result = new HashMap<>();
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();

        for (int i = 1; i <= cols; i++) {
            String colName = meta.getColumnName(i).toLowerCase();
            if (WHITELIST.contains(colName)) {
                result.put(colName, rs.getObject(i));
            }
        }

        return result;
    }

    /**
     * Versión sin ResultSet: construye un mapa sanitizado a partir de un mapa
     * existente. Útil cuando los datos ya fueron extraídos del ResultSet previamente.
     *
     * @param rawRow mapa con todos los campos de una fila
     * @return subconjunto con solo las columnas de la lista blanca
     */
    public Map<String, Object> sanitize(Map<String, Object> rawRow) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : rawRow.entrySet()) {
            if (WHITELIST.contains(entry.getKey().toLowerCase())) {
                result.put(entry.getKey().toLowerCase(), entry.getValue());
            }
        }
        return result;
    }
}
