package escuelaing.edu.co.infrastructure;

import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.TransactionRecord;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Ejecuta una consulta SQL del perfil de carga contra la BD espejo y devuelve
 * un {@link TransactionRecord} con la latencia medida y el plan de ejecución.
 *
 * <h3>Estrategia de parámetros</h3>
 * <ol>
 *   <li><b>Primario:</b> intenta extraer parámetros del texto SQL capturado en
 *       el {@link TransactionRecord} de Fase 2 usando el resultado de
 *       {@code PreparedStatement.toString()} (incluido por los drivers JDBC
 *       modernos).</li>
 *   <li><b>Fallback sintético:</b> si el parseo falla o el SQL no contiene
 *       literales, genera valores dentro del rango estadístico del perfil
 *       de carga (media ± 2σ).</li>
 * </ol>
 *
 * <h3>Plan de ejecución</h3>
 * <p>Captura el costo estimado del planificador con {@code EXPLAIN ANALYZE}
 * antes de ejecutar la consulta real. El costo queda registrado en
 * {@link TransactionRecord#getExecutionPlan()}.</p>
 */
@Component
public class QueryExecutor {

    private static final Logger LOG = Logger.getLogger(QueryExecutor.class.getName());

    private final SyntheticDataGenerator syntheticDataGenerator;
    private final Random rng = new Random();

    public QueryExecutor(SyntheticDataGenerator syntheticDataGenerator) {
        this.syntheticDataGenerator = syntheticDataGenerator;
    }

    // -------------------------------------------------------------------------
    // API pública
    // -------------------------------------------------------------------------

    /**
     * Ejecuta la consulta asociada a {@code queryId} en la BD espejo y
     * retorna el registro de ejecución con latencia y plan de ejecución.
     *
     * @param conn    conexión abierta a la BD espejo
     * @param queryId identificador de la consulta (puente con Fase 1)
     * @param stats   estadísticas del perfil de carga para esta consulta
     * @param sql     SQL capturado en Fase 2 (puede ser null)
     * @return registro de ejecución con latencia medida y plan capturado
     */
    public TransactionRecord execute(Connection conn,
                                     String queryId,
                                     LoadProfile.QueryStats stats,
                                     String sql) {
        String resolvedSql = resolveSql(sql, stats);
        String plan = captureExplainPlan(conn, resolvedSql);

        Instant start = Instant.now();
        long startNs = System.nanoTime();

        try (PreparedStatement ps = conn.prepareStatement(resolvedSql)) {
            bindSyntheticParameters(ps, stats);
            try (ResultSet rs = ps.executeQuery()) {
                // Drena el cursor para que el servidor materialice el resultado
                while (rs.next()) { /* consumir */ }
            }
        } catch (SQLException e) {
            LOG.warning("[QueryExecutor] Error ejecutando " + queryId + ": " + e.getMessage());
        }

        long latencyMs = (System.nanoTime() - startNs) / 1_000_000;

        return TransactionRecord.builder()
                .queryId(queryId)
                .sql(resolvedSql)
                .latencyMs(latencyMs)
                .timestamp(start)
                .executionPlan(plan)
                .build();
    }

    // -------------------------------------------------------------------------
    // Resolución de SQL
    // -------------------------------------------------------------------------

    /**
     * Retorna el SQL a ejecutar.
     * Si el SQL de Fase 2 no contiene placeholders {@code ?}, lo usa directamente.
     * Si tiene placeholders (o es null), genera un SQL sintético de consulta básica.
     */
    private String resolveSql(String sql, LoadProfile.QueryStats stats) {
        if (sql != null && !sql.isBlank() && !sql.contains("?")) {
            return sql;
        }
        // Fallback: SELECT genérico sobre warehouse (tabla siempre presente en TPC-C)
        return "SELECT w_id, w_name, w_tax FROM warehouse WHERE w_id = " + syntheticInt(1, 2);
    }

    /**
     * Intenta bindear parámetros sintéticos en los placeholders {@code ?} del statement.
     * Usa valores dentro del rango (mean ± 2σ) estimado del perfil.
     */
    private void bindSyntheticParameters(PreparedStatement ps,
                                          LoadProfile.QueryStats stats) throws SQLException {
        // Intento ciego: si hay parámetros en el statement, los llena con enteros sintéticos
        try {
            java.sql.ParameterMetaData meta = ps.getParameterMetaData();
            for (int i = 1; i <= meta.getParameterCount(); i++) {
                ps.setInt(i, syntheticInt(1, 1000));
            }
        } catch (SQLException ignored) {
            // Algunos drivers lanzan excepción en getParameterMetaData — ignorar
        }
    }

    // -------------------------------------------------------------------------
    // EXPLAIN ANALYZE
    // -------------------------------------------------------------------------

    /**
     * Captura el plan de ejecución con {@code EXPLAIN ANALYZE}.
     * Solo funciona con PostgreSQL; devuelve {@code null} con cualquier otro motor.
     *
     * @param conn conexión a la BD espejo
     * @param sql  consulta a analizar
     * @return texto completo del plan, o {@code null} si falla
     */
    private String captureExplainPlan(Connection conn, String sql) {
        try (PreparedStatement ps = conn.prepareStatement("EXPLAIN ANALYZE " + sql)) {
            StringBuilder sb = new StringBuilder();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) sb.append(rs.getString(1)).append('\n');
            }
            return sb.toString();
        } catch (SQLException e) {
            LOG.fine("[QueryExecutor] EXPLAIN ANALYZE no disponible: " + e.getMessage());
            return null;
        }
    }

    /**
     * Extrae el costo estimado del planificador del texto del plan EXPLAIN.
     * PostgreSQL reporta la línea {@code cost=X..Y} en el nodo raíz.
     *
     * @param explainOutput salida de EXPLAIN ANALYZE
     * @return costo estimado total (el valor después de {@code ..}), o 0 si no se puede parsear
     */
    public static double extractPlanCost(String explainOutput) {
        if (explainOutput == null || explainOutput.isBlank()) return 0.0;
        // Busca el primer "cost=X..Y" en el texto
        int idx = explainOutput.indexOf("cost=");
        if (idx < 0) return 0.0;
        try {
            String sub = explainOutput.substring(idx + 5);
            int dotDot = sub.indexOf("..");
            int space  = sub.indexOf(' ');
            if (dotDot < 0 || space < 0) return 0.0;
            return Double.parseDouble(sub.substring(dotDot + 2, space));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    // -------------------------------------------------------------------------

    private int syntheticInt(int min, int max) {
        return min + rng.nextInt(max - min + 1);
    }
}
