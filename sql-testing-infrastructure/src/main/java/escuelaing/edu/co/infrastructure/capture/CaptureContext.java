package escuelaing.edu.co.infrastructure.capture;

/**
 * Binds the active {@code queryId} to the current thread via a {@link ThreadLocal}, so
 * {@link JdbcWrapper} can link each JDBC execution to its query without changing method
 * signatures. State is thread-confined: work handed to another thread is not captured.
 *
 * <p>Always use inside a {@code try-with-resources} block to guarantee cleanup:</p>
 * <pre>{@code
 * try (CaptureContext ctx = CaptureContext.begin("getUserOrders")) {
 *     return jdbcTemplate.query(SQL, rowMapper);
 * }
 * }</pre>
 *
 * <p>For warmup capture, where sampling must be bypassed, use {@link #beginForced}. The
 * forced flag survives nested {@link #begin} calls and is cleared only when the forced
 * context that set it closes:</p>
 * <pre>{@code
 * try (CaptureContext ctx = CaptureContext.beginForced("getUserOrders")) {
 *     repository.getUserOrders(userId);   // SamplingFilter always records
 * }
 * }</pre>
 *
 * <p>If {@link JdbcWrapper} executes outside a {@code CaptureContext},
 * {@link #currentQueryId()} returns {@code null} and nothing is recorded. Closing any
 * context clears the {@code queryId}; an outer context's id is not restored after a nested
 * one closes.</p>
 */
public final class CaptureContext implements AutoCloseable {

    private static final ThreadLocal<String>  CURRENT_QUERY_ID = new ThreadLocal<>();
    private static final ThreadLocal<Boolean> FORCE_CAPTURE    = new ThreadLocal<>();

    private final boolean ownsForceFlag;

    private CaptureContext(String queryId, boolean forced) {
        CURRENT_QUERY_ID.set(queryId);
        this.ownsForceFlag = forced;
        if (forced) {
            FORCE_CAPTURE.set(Boolean.TRUE);
        }
    }

    /**
     * Opens a capture context for the given {@code queryId} on the current thread.
     * Must always be used in a {@code try-with-resources} block.
     *
     * @param queryId identifier declared in {@code @SqlQuery#queryId}
     * @return the open context (closed automatically on try exit)
     */
    public static CaptureContext begin(String queryId) {
        return new CaptureContext(queryId, false);
    }

    /**
     * Opens a forced capture context: {@link SamplingFilter} records every execution
     * regardless of priority or latency. Use during warmup so every declared query is
     * captured at least once. Inner {@link #begin} calls do not clear the forced flag.
     *
     * @param queryId identifier declared in {@code @SqlQuery#queryId}
     * @return the open forced context (closed automatically on try exit)
     */
    public static CaptureContext beginForced(String queryId) {
        return new CaptureContext(queryId, true);
    }

    /**
     * Returns the {@code queryId} active on the current thread,
     * or {@code null} if no context is open.
     */
    public static String currentQueryId() {
        return CURRENT_QUERY_ID.get();
    }

    /** Returns {@code true} if the current thread is inside a forced capture context. */
    public static boolean isForced() {
        return Boolean.TRUE.equals(FORCE_CAPTURE.get());
    }

    /**
     * Clears the current thread's {@code queryId}, and the forced flag only if this
     * context set it.
     */
    @Override
    public void close() {
        CURRENT_QUERY_ID.remove();
        if (ownsForceFlag) {
            FORCE_CAPTURE.remove();
        }
    }
}
