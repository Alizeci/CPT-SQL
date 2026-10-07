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
 * Driver-level JDBC interceptor that records the SQL text and latency of instrumented
 * {@link PreparedStatement} executions in the capture environment.
 *
 * <p>Wrap a real {@link Connection} with {@link #wrap(Connection)}; every
 * {@code prepareStatement} / {@code prepareCall} then returns a {@link PreparedStatement}
 * proxy. While {@link CaptureToggle} is on, each {@code execute*} call is timed and, if
 * {@link SamplingFilter} accepts it, a {@link TransactionRecord} (query id, SQL, latency in
 * ms, update count) is enqueued in {@link MetricsBuffer}. For queries, the first row read is
 * passed through {@link SanitizationStrategy} and attached to the record.</p>
 *
 * <p>The active {@code queryId} is read from {@link CaptureContext#currentQueryId()};
 * without an open context nothing is recorded. Failed executions are still recorded when
 * sampled; the original exception is then rethrown.</p>
 */
@Component
public class JdbcWrapper {

    private static final Logger LOG = Logger.getLogger(JdbcWrapper.class.getName());

    private static final Set<String> EXECUTE_METHODS = Set.of(
            "execute", "executeQuery", "executeUpdate", "executeLargeUpdate"
    );

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

    /**
     * Wraps a real {@link Connection} with the capture decorator.
     *
     * @param real the original JDBC connection
     * @return an instrumented {@link Connection} proxy
     */
    public Connection wrap(Connection real) {
        return (Connection) Proxy.newProxyInstance(
                real.getClass().getClassLoader(),
                new Class[]{Connection.class},
                new ConnectionHandler(real)
        );
    }

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

            // Record even when execution fails (e.g. statement_timeout): the captured SQL is
            // what the benchmark later runs against the mirror database.
            Throwable executionError = null;
            Object result = null;
            try {
                result = invokeDelegate(delegate, method, args);
            } catch (Throwable t) {
                executionError = t;
            }

            long latencyMs = (System.nanoTime() - startNano) / 1_000_000;

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

                if (executionError == null && result instanceof ResultSet) {
                    result = wrapResultSetForCapture((ResultSet) result, record);
                }
            }

            if (executionError != null) throw executionError;
            return result;
        }
    }

    /**
     * Wraps a {@link ResultSet} so the first row is sanitized via {@link SanitizationStrategy}
     * on the first successful {@code next()}. The caller's cursor is unaffected; sanitization
     * failures are logged and ignored.
     */
    private ResultSet wrapResultSetForCapture(ResultSet real, TransactionRecord record) {
        return (ResultSet) Proxy.newProxyInstance(
                real.getClass().getClassLoader(),
                new Class[]{ResultSet.class},
                new ResultSetCaptureHandler(real, record)
        );
    }

    private class ResultSetCaptureHandler implements InvocationHandler {

        private final ResultSet        delegate;
        private final TransactionRecord record;
        private boolean captured = false;

        ResultSetCaptureHandler(ResultSet delegate, TransactionRecord record) {
            this.delegate = delegate;
            this.record   = record;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object result = invokeDelegate(delegate, method, args);

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
                    LOG.fine("[JdbcWrapper] Row sanitization skipped: " + e.getMessage());
                }
            }

            return result;
        }
    }

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
