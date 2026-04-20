package escuelaing.edu.co.infrastructure.capture;

/**
 * Propagates the active {@code queryId} to the current thread via a
 * {@link ThreadLocal}, allowing {@link JdbcWrapper} to associate each JDBC
 * execution with a query without modifying method signatures.
 *
 * <p>Always use inside a {@code try-with-resources} block to guarantee cleanup:</p>
 * <pre>{@code
 * try (CaptureContext ctx = CaptureContext.begin("getUserOrders")) {
 *     return jdbcTemplate.query(SQL, rowMapper);
 * }
 * }</pre>
 *
 * <p>If {@link JdbcWrapper} executes outside a {@code CaptureContext},
 * {@link #currentQueryId()} returns {@code null} and capture is skipped.</p>
 */
public final class CaptureContext implements AutoCloseable {

    private static final ThreadLocal<String> CURRENT_QUERY_ID = new ThreadLocal<>();

    private CaptureContext(String queryId) {
        CURRENT_QUERY_ID.set(queryId);
    }

    /**
     * Opens a capture context for the given {@code queryId} on the current thread.
     * Must always be used in a {@code try-with-resources} block.
     *
     * @param queryId identifier declared in {@code @SqlQuery#queryId}
     * @return the open context (closed automatically on try exit)
     */
    public static CaptureContext begin(String queryId) {
        return new CaptureContext(queryId);
    }

    /**
     * Returns the {@code queryId} active on the current thread,
     * or {@code null} if no context is open.
     */
    public static String currentQueryId() {
        return CURRENT_QUERY_ID.get();
    }

    /** Removes the {@code queryId} from the current thread's {@link ThreadLocal}. */
    @Override
    public void close() {
        CURRENT_QUERY_ID.remove();
    }
}
