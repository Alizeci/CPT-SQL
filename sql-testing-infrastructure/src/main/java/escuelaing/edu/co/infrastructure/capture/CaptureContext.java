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
 * <p>For warmup capture where sampling must be bypassed unconditionally,
 * use {@link #beginForced}. The forced flag is sticky across nested {@link #begin}
 * calls and is only cleared when the outermost forced context closes:</p>
 * <pre>{@code
 * try (CaptureContext ctx = CaptureContext.beginForced("getUserOrders")) {
 *     repository.getUserOrders(userId);   // SamplingFilter always records
 * }
 * }</pre>
 *
 * <p>If {@link JdbcWrapper} executes outside a {@code CaptureContext},
 * {@link #currentQueryId()} returns {@code null} and capture is skipped.</p>
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
     * Opens a forced capture context: {@link SamplingFilter} bypasses all sampling
     * rules and records the execution unconditionally. Use during warmup to guarantee
     * every query in the contract is captured at least once regardless of priority or
     * latency. Inner {@link #begin} calls do not clear the forced flag; only the
     * outermost forced context clears it on close.
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

    /**
     * Returns {@code true} if the current thread is inside a forced capture context.
     * Checked by {@link SamplingFilter} to bypass sampling rules unconditionally.
     */
    public static boolean isForced() {
        return Boolean.TRUE.equals(FORCE_CAPTURE.get());
    }

    /**
     * Removes the {@code queryId} from the current thread's {@link ThreadLocal}.
     * Only clears the forced flag if this context was the one that set it.
     */
    @Override
    public void close() {
        CURRENT_QUERY_ID.remove();
        if (ownsForceFlag) {
            FORCE_CAPTURE.remove();
        }
    }
}
