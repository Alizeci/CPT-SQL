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
 * JDBC decorator that intercepts {@link PreparedStatement} execution to measure
 * latency and capture sanitized row data.
 *
 * <p>Wrap a real {@link Connection} with {@link #wrap(Connection)}; from that point,
 * every {@code prepareStatement} / {@code prepareCall} returns a proxy that measures
 * execution time, applies {@link SamplingFilter}, and enqueues a
 * {@link TransactionRecord} in {@link MetricsBuffer}.</p>
 *
 * <p>The active {@code queryId} is read from {@link CaptureContext#currentQueryId()}.
 * If no context is open, the call is passed through without instrumentation.</p>
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

    // Public API

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

    // Proxy handlers

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

            // Execute the query and capture SQL even on failure (e.g. statement_timeout).
            // The captured SQL is required for phases 3+4 to benchmark the PR's query.
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
     * Wraps a {@link ResultSet} to capture and sanitize the first row via
     * {@link SanitizationStrategy} on the first successful {@code next()} call.
     * The caller receives the cursor positioned normally and can read all columns.
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
