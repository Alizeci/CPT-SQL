package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.TransactionRecord;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Decorador JDBC que intercepta la creación de {@link PreparedStatement}s y
 * mide la latencia de cada ejecución.
 *
 * <h3>Funcionamiento</h3>
 * <ol>
 *   <li>El desarrollador envuelve su {@link Connection} real con
 *       {@link #wrap(Connection)}.</li>
 *   <li>Cada llamada a {@code prepareStatement(...)} devuelve un proxy que
 *       intercepta los métodos {@code execute}, {@code executeQuery} y
 *       {@code executeUpdate}.</li>
 *   <li>Antes y después de la ejecución real se mide la latencia.</li>
 *   <li>{@link SamplingFilter} decide si la muestra se registra.</li>
 *   <li>Si procede, se construye un {@link TransactionRecord} y se encola en
 *       el {@link MetricsBuffer}.</li>
 * </ol>
 *
 * <p>Se usa {@link java.lang.reflect.Proxy} para delegar automáticamente todos
 * los métodos que no requieren instrumentación, evitando implementar los ~50
 * métodos de las interfaces {@link Connection} y {@link PreparedStatement}.</p>
 *
 * <p>El {@code queryId} activo se obtiene de {@link CaptureContext#currentQueryId()}.
 * Si no hay contexto abierto, la captura se omite.</p>
 */
@Component
public class JdbcWrapper {

    private static final Logger LOG = Logger.getLogger(JdbcWrapper.class.getName());

    /** Nombres de métodos de PreparedStatement que ejecutan la consulta. */
    private static final Set<String> EXECUTE_METHODS = Set.of(
            "execute", "executeQuery", "executeUpdate", "executeLargeUpdate"
    );

    /** Nombres de métodos de Connection que crean un PreparedStatement. */
    private static final Set<String> PREPARE_METHODS = Set.of(
            "prepareStatement", "prepareCall"
    );

    private final SamplingFilter       samplingFilter;
    private final MetricsBuffer        metricsBuffer;
    private final CaptureToggle        captureToggle;
    private final SanitizationStrategy sanitizationStrategy;

    public JdbcWrapper(SamplingFilter samplingFilter,
                       MetricsBuffer metricsBuffer,
                       CaptureToggle captureToggle,
                       SanitizationStrategy sanitizationStrategy) {
        this.samplingFilter       = samplingFilter;
        this.metricsBuffer        = metricsBuffer;
        this.captureToggle        = captureToggle;
        this.sanitizationStrategy = sanitizationStrategy;
    }

    // -------------------------------------------------------------------------
    // API pública
    // -------------------------------------------------------------------------

    /**
     * Envuelve una {@link Connection} real con el wrapper de captura.
     *
     * @param real la conexión JDBC original
     * @return una {@link Connection} proxy instrumentada
     */
    public Connection wrap(Connection real) {
        return (Connection) Proxy.newProxyInstance(
                real.getClass().getClassLoader(),
                new Class[]{Connection.class},
                new ConnectionHandler(real)
        );
    }

    // -------------------------------------------------------------------------
    // Handlers internos
    // -------------------------------------------------------------------------

    /** Intercepta las llamadas a {@link Connection}. */
    private class ConnectionHandler implements InvocationHandler {

        private final Connection delegate;

        ConnectionHandler(Connection delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (PREPARE_METHODS.contains(method.getName())) {
                String sql = (args != null && args.length > 0 && args[0] instanceof String)
                        ? (String) args[0]
                        : "<unknown>";
                PreparedStatement realPs = (PreparedStatement) invokeDelegate(delegate, method, args);
                return wrapStatement(realPs, sql);
            }
            return invokeDelegate(delegate, method, args);
        }
    }

    /** Intercepta las llamadas a {@link PreparedStatement}. */
    private class StatementHandler implements InvocationHandler {

        private final PreparedStatement delegate;
        private final String sql;

        StatementHandler(PreparedStatement delegate, String sql) {
            this.delegate = delegate;
            this.sql      = sql;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (captureToggle.isEnabled() && EXECUTE_METHODS.contains(method.getName())) {
                return executeWithCapture(method, args);
            }
            return invokeDelegate(delegate, method, args);
        }

        private Object executeWithCapture(Method method, Object[] args) throws Throwable {
            String queryId = CaptureContext.currentQueryId();
            Instant start  = Instant.now();
            long startNano = System.nanoTime();

            // Ejecutar la query; capturar el SQL incluso si falla (ej. statement_timeout).
            // El capturedSql es esencial para que Fases 3+4 benchmarkeen el SQL del PR.
            Throwable executionError = null;
            Object result = null;
            try {
                result = invokeDelegate(delegate, method, args);
            } catch (Throwable t) {
                executionError = t;
            }

            long latencyMs = (System.nanoTime() - startNano) / 1_000_000;

            // Captura de filas afectadas para operaciones de escritura
            long rowCount = 0;
            if (executionError == null) {
                String mn = method.getName();
                if ("executeUpdate".equals(mn) && result instanceof Integer) {
                    rowCount = (Integer) result;
                } else if ("executeLargeUpdate".equals(mn) && result instanceof Long) {
                    rowCount = (Long) result;
                } else if ("execute".equals(mn) && result instanceof Boolean && !(Boolean) result) {
                    try { rowCount = delegate.getUpdateCount(); } catch (Exception ignored) {}
                }
            }

            if (queryId != null && samplingFilter.shouldRecord(queryId, latencyMs)) {
                TransactionRecord record = TransactionRecord.builder()
                        .queryId(queryId)
                        .sql(sql)
                        .latencyMs(latencyMs)
                        .rowCount(rowCount)
                        .timestamp(start)
                        .build();
                metricsBuffer.record(record);

                // Captura diferida: envuelve el ResultSet en un proxy que sanitiza
                // la primera fila cuando el caller la lee (sin consumir el cursor).
                if (executionError == null && result instanceof ResultSet) {
                    result = wrapResultSetForCapture((ResultSet) result, record);
                }
            }

            if (executionError != null) throw executionError;
            return result;
        }
    }

    // -------------------------------------------------------------------------
    // ResultSet proxy — captura diferida de fila sanitizada
    // -------------------------------------------------------------------------

    /**
     * Envuelve un {@link ResultSet} real en un proxy que intercepta la primera
     * llamada exitosa a {@code next()} para capturar la fila actual mediante
     * {@link SanitizationStrategy} y almacenarla en el {@link TransactionRecord}
     * ya encolado.
     *
     * <p>La captura es <em>no destructiva</em>: el caller recibe el cursor
     * posicionado en la fila normalmente y puede leer todas las columnas sin
     * restricciones.</p>
     *
     * @param real   ResultSet original devuelto por el driver JDBC
     * @param record TransactionRecord ya registrado en el buffer (mutable vía setter)
     */
    private ResultSet wrapResultSetForCapture(ResultSet real, TransactionRecord record) {
        return (ResultSet) Proxy.newProxyInstance(
                real.getClass().getClassLoader(),
                new Class[]{ResultSet.class},
                new ResultSetCaptureHandler(real, record)
        );
    }

    /** Intercepta {@code next()} para capturar la primera fila sanitizada. */
    private class ResultSetCaptureHandler implements InvocationHandler {

        private final ResultSet       delegate;
        private final TransactionRecord record;
        private boolean captured = false;

        ResultSetCaptureHandler(ResultSet delegate, TransactionRecord record) {
            this.delegate = delegate;
            this.record   = record;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object result = invokeDelegate(delegate, method, args);

            // Solo en la primera fila leída: sanitizar y adjuntar al record
            if (!captured
                    && "next".equals(method.getName())
                    && Boolean.TRUE.equals(result)) {
                captured = true;
                try {
                    Map<String, Object> sanitized = sanitizationStrategy.sanitize(delegate);
                    if (!sanitized.isEmpty()) {
                        record.setSanitizedData(sanitized);
                    }
                } catch (Exception e) {
                    LOG.fine("[JdbcWrapper] Sanitización de fila omitida: " + e.getMessage());
                }
            }

            return result;
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private PreparedStatement wrapStatement(PreparedStatement real, String sql) {
        return (PreparedStatement) Proxy.newProxyInstance(
                real.getClass().getClassLoader(),
                new Class[]{PreparedStatement.class},
                new StatementHandler(real, sql)
        );
    }

    private static Object invokeDelegate(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
