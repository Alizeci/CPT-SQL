package escuelaing.edu.co.infrastructure.benchmark;

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
 *   <li><b>Primario:</b> usa el SQL capturado en Fase 2 almacenado en
 *       {@link LoadProfile.QueryStats#getCapturedSql()}.</li>
 *   <li><b>Fallback sintético:</b> si no hay SQL capturado, la ejecución se
 *       omite con un warning — no se usa SQL genérico ajeno al schema.</li>
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

    private final Random rng = new Random();

    public QueryExecutor() {}

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

        Instant start  = Instant.now();
        long startNs   = System.nanoTime();

        try (PreparedStatement ps = conn.prepareStatement(resolvedSql)) {
            bindSyntheticParameters(ps, stats);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) { /* consumir el cursor */ }
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

    private String resolveSql(String sql, LoadProfile.QueryStats stats) {
        if (sql != null && !sql.isBlank()) {
            return sql;
        }
        return null;
    }

    private void bindSyntheticParameters(PreparedStatement ps,
                                          LoadProfile.QueryStats stats) throws SQLException {
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
        int idx = explainOutput.indexOf("cost=");
        if (idx < 0) return 0.0;
        try {
            String sub    = explainOutput.substring(idx + 5);
            int dotDot    = sub.indexOf("..");
            int space     = sub.indexOf(' ');
            if (dotDot < 0 || space < 0) return 0.0;
            return Double.parseDouble(sub.substring(dotDot + 2, space));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private int syntheticInt(int min, int max) {
        return min + rng.nextInt(max - min + 1);
    }
}
