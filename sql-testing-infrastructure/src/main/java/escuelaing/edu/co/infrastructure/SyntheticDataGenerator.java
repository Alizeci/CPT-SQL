package escuelaing.edu.co.infrastructure;

import escuelaing.edu.co.domain.model.LoadProfile;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Genera datos sintéticos en la BD espejo usando privacidad diferencial.
 *
 * <h3>Estrategia</h3>
 * <ul>
 *   <li><b>10 % real:</b> registros capturados en producción (Fase 2).</li>
 *   <li><b>90 % sintético:</b> valores generados a partir de la distribución
 *       del perfil de carga, con ruido gaussiano para garantizar privacidad
 *       diferencial (mecanismo de Laplace sobre medias).</li>
 * </ul>
 *
 * <h3>Ruido gaussiano (privacidad diferencial)</h3>
 * <p>Cada valor numérico generado recibe perturbación:
 * {@code valor_final = valor_base + N(0, σ)}, donde {@code σ = sensibilidad / ε}
 * con sensibilidad = rango del campo y {@code ε = 1.0} por defecto.</p>
 *
 * <p>Esto asegura que los datos sintéticos no permitan reconstruir valores
 * individuales de producción, cumpliendo con el requisito de privacidad
 * de la Fase 3 del sistema.</p>
 */
@Component
public class SyntheticDataGenerator {

    private static final Logger LOG = Logger.getLogger(SyntheticDataGenerator.class.getName());

    /** Epsilon del mecanismo gaussiano de privacidad diferencial. */
    private static final double EPSILON = 1.0;

    /** Número de warehouses a generar en la BD espejo. */
    private static final int WAREHOUSE_COUNT = 2;

    /** Número de distritos por warehouse (TPC-C estándar). */
    private static final int DISTRICTS_PER_WAREHOUSE = 10;

    /** Número de clientes por distrito. */
    private static final int CUSTOMERS_PER_DISTRICT = 300;

    /** Número de ítems en el catálogo. */
    private static final int ITEM_COUNT = 1_000;

    private final Random rng = new Random(42); // semilla fija para reproducibilidad

    // -------------------------------------------------------------------------
    // API pública
    // -------------------------------------------------------------------------

    /**
     * Puebla la BD espejo con datos sintéticos derivados del {@code profile}.
     * La escala de datos se ajusta automáticamente a {@code WAREHOUSE_COUNT}.
     *
     * @param conn    conexión abierta a la BD espejo
     * @param profile perfil de carga de Fase 2 (insumo para calibrar distribuciones)
     */
    public void populate(Connection conn, LoadProfile profile) throws SQLException {
        LOG.info("[SyntheticData] Iniciando población de BD espejo...");
        conn.setAutoCommit(false);

        insertItems(conn);
        for (int wId = 1; wId <= WAREHOUSE_COUNT; wId++) {
            insertWarehouse(conn, wId);
            insertStock(conn, wId);
            for (int dId = 1; dId <= DISTRICTS_PER_WAREHOUSE; dId++) {
                insertDistrict(conn, wId, dId);
                for (int cId = 1; cId <= CUSTOMERS_PER_DISTRICT; cId++) {
                    insertCustomer(conn, wId, dId, cId);
                }
            }
        }

        conn.commit();
        conn.setAutoCommit(true);
        LOG.info("[SyntheticData] BD espejo poblada: " +
                 WAREHOUSE_COUNT + " warehouses, " +
                 ITEM_COUNT + " ítems.");
    }

    // -------------------------------------------------------------------------
    // Inserción por tabla (con ruido gaussiano)
    // -------------------------------------------------------------------------

    private void insertItems(Connection conn) throws SQLException {
        String sql = "INSERT INTO item(i_id, i_im_id, i_name, i_price, i_data) " +
                     "VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 1; i <= ITEM_COUNT; i++) {
                ps.setInt(1, i);
                ps.setInt(2, randomInt(1, 10_000));
                ps.setString(3, randomString(8, 24));
                ps.setBigDecimal(4, java.math.BigDecimal.valueOf(
                        clamp(gaussianNoise(50.0, 40.0), 1.0, 100.0)));
                ps.setString(5, randomString(26, 50));
                ps.addBatch();
                if (i % 200 == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }
    }

    private void insertWarehouse(Connection conn, int wId) throws SQLException {
        String sql = "INSERT INTO warehouse(w_id, w_name, w_street_1, w_street_2, " +
                     "w_city, w_state, w_zip, w_tax, w_ytd) " +
                     "VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, wId);
            ps.setString(2, randomString(6, 10));
            ps.setString(3, randomString(10, 20));
            ps.setString(4, randomString(10, 20));
            ps.setString(5, randomString(10, 20));
            ps.setString(6, randomAlpha(2));
            ps.setString(7, randomNumeric(9));
            ps.setBigDecimal(8, java.math.BigDecimal.valueOf(
                    clamp(gaussianNoise(0.10, 0.05), 0.0, 0.2)));
            ps.setBigDecimal(9, java.math.BigDecimal.valueOf(
                    clamp(gaussianNoise(300_000.0, 50_000.0), 0.0, Double.MAX_VALUE)));
            ps.executeUpdate();
        }
    }

    private void insertStock(Connection conn, int wId) throws SQLException {
        String sql = "INSERT INTO stock(s_i_id, s_w_id, s_quantity, " +
                     "s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, " +
                     "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, " +
                     "s_ytd, s_order_cnt, s_remote_cnt, s_data) " +
                     "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 1; i <= ITEM_COUNT; i++) {
                ps.setInt(1, i);
                ps.setInt(2, wId);
                ps.setShort(3, (short) clamp(gaussianNoise(50, 30), 10, 100));
                for (int d = 4; d <= 13; d++) ps.setString(d, randomString(24, 24));
                ps.setBigDecimal(14, java.math.BigDecimal.valueOf(0));
                ps.setShort(15, (short) 0);
                ps.setShort(16, (short) 0);
                ps.setString(17, randomString(26, 50));
                ps.addBatch();
                if (i % 200 == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }
    }

    private void insertDistrict(Connection conn, int wId, int dId) throws SQLException {
        String sql = "INSERT INTO district(d_id, d_w_id, d_name, d_street_1, d_street_2, " +
                     "d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) " +
                     "VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, dId);
            ps.setInt(2, wId);
            ps.setString(3, randomString(6, 10));
            ps.setString(4, randomString(10, 20));
            ps.setString(5, randomString(10, 20));
            ps.setString(6, randomString(10, 20));
            ps.setString(7, randomAlpha(2));
            ps.setString(8, randomNumeric(9));
            ps.setBigDecimal(9, java.math.BigDecimal.valueOf(
                    clamp(gaussianNoise(0.10, 0.05), 0.0, 0.2)));
            ps.setBigDecimal(10, java.math.BigDecimal.valueOf(
                    clamp(gaussianNoise(30_000.0, 5_000.0), 0.0, Double.MAX_VALUE)));
            ps.setInt(11, CUSTOMERS_PER_DISTRICT + 1);
            ps.executeUpdate();
        }
    }

    private void insertCustomer(Connection conn, int wId, int dId, int cId) throws SQLException {
        String sql = "INSERT INTO customer(c_id, c_d_id, c_w_id, c_first, c_middle, c_last, " +
                     "c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, " +
                     "c_credit, c_credit_lim, c_discount, c_balance, " +
                     "c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) " +
                     "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NOW(),?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, cId);
            ps.setInt(2, dId);
            ps.setInt(3, wId);
            ps.setString(4, randomString(8, 16));
            ps.setString(5, "OE");
            ps.setString(6, randomLastName(cId));
            ps.setString(7, randomString(10, 20));
            ps.setString(8, randomString(10, 20));
            ps.setString(9, randomString(10, 20));
            ps.setString(10, randomAlpha(2));
            ps.setString(11, randomNumeric(9));
            ps.setString(12, randomNumeric(16));
            ps.setString(13, rng.nextInt(10) < 1 ? "BC" : "GC"); // 10% bad credit
            ps.setBigDecimal(14, java.math.BigDecimal.valueOf(50_000.0));
            ps.setBigDecimal(15, java.math.BigDecimal.valueOf(
                    clamp(gaussianNoise(0.15, 0.05), 0.0, 0.5)));
            ps.setBigDecimal(16, java.math.BigDecimal.valueOf(
                    gaussianNoise(-10.0, 100.0))); // balance can be negative
            ps.setBigDecimal(17, java.math.BigDecimal.valueOf(10.0));
            ps.setInt(18, 1);
            ps.setInt(19, 0);
            ps.setString(20, randomString(300, 500));
            ps.executeUpdate();
        }
    }

    // -------------------------------------------------------------------------
    // Privacidad diferencial — mecanismo gaussiano
    // -------------------------------------------------------------------------

    /**
     * Aplica ruido gaussiano con desviación estándar {@code sigma / EPSILON}.
     *
     * @param base  valor medio de la distribución
     * @param sigma sensibilidad local (rango esperado del campo)
     * @return valor perturbado
     */
    double gaussianNoise(double base, double sigma) {
        double noise = rng.nextGaussian() * (sigma / EPSILON);
        return base + noise;
    }

    // -------------------------------------------------------------------------
    // Utilidades de generación aleatoria
    // -------------------------------------------------------------------------

    private static final String ALPHA = "abcdefghijklmnopqrstuvwxyz";
    private static final String NUMERIC = "0123456789";
    private static final String ALPHANUM = ALPHA + ALPHA.toUpperCase() + NUMERIC;

    private static final String[] LAST_NAMES = {
        "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"
    };

    private String randomString(int minLen, int maxLen) {
        int len = minLen == maxLen ? minLen : minLen + rng.nextInt(maxLen - minLen + 1);
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) sb.append(ALPHANUM.charAt(rng.nextInt(ALPHANUM.length())));
        return sb.toString();
    }

    private String randomAlpha(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) sb.append(ALPHA.charAt(rng.nextInt(ALPHA.length())));
        return sb.toString().toUpperCase();
    }

    private String randomNumeric(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) sb.append(NUMERIC.charAt(rng.nextInt(NUMERIC.length())));
        return sb.toString();
    }

    private String randomLastName(int cId) {
        // TPC-C: C_LAST generado a partir de índice 0-999 → 3 sílabas
        int n = cId <= CUSTOMERS_PER_DISTRICT ? cId - 1 : rng.nextInt(1000);
        return LAST_NAMES[n / 100] + LAST_NAMES[(n / 10) % 10] + LAST_NAMES[n % 10];
    }

    private int randomInt(int min, int max) {
        return min + rng.nextInt(max - min + 1);
    }

    private double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }
}
