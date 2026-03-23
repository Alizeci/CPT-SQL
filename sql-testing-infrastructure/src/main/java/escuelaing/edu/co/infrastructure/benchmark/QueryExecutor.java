package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.TestProfile;
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
     * Ejecuta la consulta asociada a {@code queryId} en la BD espejo con
     * distribución de acceso UNIFORM (parámetros sintéticos uniformes).
     */
    public TransactionRecord execute(Connection conn,
                                     String queryId,
                                     LoadProfile.QueryStats stats,
                                     String sql) {
        return execute(conn, queryId, stats, sql,
                TestProfile.AccessDistribution.UNIFORM, 1.0);
    }

    /**
     * Ejecuta la consulta asociada a {@code queryId} en la BD espejo y
     * retorna el registro de ejecución con latencia y plan de ejecución.
     *
     * <p>Los parámetros sintéticos se vinculan según la distribución de acceso
     * indicada. Con {@link TestProfile.AccessDistribution#ZIPF} los valores
     * se sesgan hacia los registros más populares (hot spots), replicando el
     * comportamiento de un flash sale (BenchPress §2 "time-evolving access skew").</p>
     *
     * @param conn       conexión abierta a la BD espejo
     * @param queryId    identificador de la consulta (puente con Fase 1)
     * @param stats      estadísticas del perfil de carga para esta consulta
     * @param sql        SQL capturado en Fase 2 (puede ser null — se omite si stats tampoco tiene)
     * @param accessDist distribución de acceso a los datos
     * @param zipfAlpha  parámetro α de Zipf; ignorado si la distribución es UNIFORM
     * @return registro de ejecución con latencia medida y plan capturado
     */
    public TransactionRecord execute(Connection conn,
                                     String queryId,
                                     LoadProfile.QueryStats stats,
                                     String sql,
                                     TestProfile.AccessDistribution accessDist,
                                     double zipfAlpha) {
        String resolvedSql = resolveSql(sql, stats);
        if (resolvedSql == null) {
            LOG.warning("[QueryExecutor] Sin SQL para " + queryId + " — ejecución omitida.");
            return TransactionRecord.builder()
                    .queryId(queryId).sql("").latencyMs(0).timestamp(Instant.now()).build();
        }

        String plan = captureExplainPlan(conn, resolvedSql);

        Instant start  = Instant.now();
        long startNs   = System.nanoTime();

        try (PreparedStatement ps = conn.prepareStatement(resolvedSql)) {
            bindSyntheticParameters(ps, accessDist, zipfAlpha);
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
        if (stats != null && stats.getCapturedSql() != null && !stats.getCapturedSql().isBlank()) {
            return stats.getCapturedSql();
        }
        return null;
    }

    /**
     * Vincula parámetros sintéticos al {@link PreparedStatement}.
     *
     * <p>Con {@link TestProfile.AccessDistribution#ZIPF} los valores se sesgan
     * hacia los registros más populares usando un generador Zipf inverso,
     * simulando hot spots de acceso (BenchPress §2, Dyn-YCSB Fig. 1).</p>
     */
    private void bindSyntheticParameters(PreparedStatement ps,
                                          TestProfile.AccessDistribution dist,
                                          double zipfAlpha) throws SQLException {
        try {
            java.sql.ParameterMetaData meta = ps.getParameterMetaData();
            for (int i = 1; i <= meta.getParameterCount(); i++) {
                int value = (dist == TestProfile.AccessDistribution.ZIPF)
                        ? zipfInt(1_000, zipfAlpha)
                        : syntheticInt(1, 1_000);
                ps.setInt(i, value);
            }
        } catch (SQLException ignored) {
            // Algunos drivers lanzan excepción en getParameterMetaData — ignorar
        }
    }

    /**
     * Genera un entero aleatorio sesgado según la distribución Zipf(α) en [1, n].
     *
     * <p>P(k) ∝ k^{-α}: los valores bajos (registros "populares") se seleccionan
     * con mayor frecuencia. Para α=1.0 el top-20 % recibe ~80 % de los accesos.</p>
     *
     * <p>Implementación por transformada inversa sobre CDF normalizada (O(n) por llamada,
     * aceptable para n=1000 en el contexto de un benchmark).</p>
     */
    private int zipfInt(int n, double alpha) {
        double sum = 0;
        for (int i = 1; i <= n; i++) sum += 1.0 / Math.pow(i, alpha);
        double target = rng.nextDouble() * sum;
        double cumulative = 0;
        for (int i = 1; i <= n; i++) {
            cumulative += 1.0 / Math.pow(i, alpha);
            if (cumulative >= target) return i;
        }
        return n;
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
