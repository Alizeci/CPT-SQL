package escuelaing.edu.co.infrastructure.capture;

/**
 * Transporta el {@code queryId} activo al hilo de ejecución actual mediante
 * un {@link ThreadLocal}, permitiendo que el wrapper JDBC conozca la consulta
 * que está siendo ejecutada sin requerir parámetros adicionales en la firma
 * de los métodos JDBC.
 *
 * <h3>Uso</h3>
 * <pre>{@code
 * try (CaptureContext ctx = CaptureContext.begin("getUserOrders")) {
 *     return jdbcTemplate.query(SQL, rowMapper);
 * }
 * }</pre>
 *
 * <p>El bloque {@code try-with-resources} garantiza que el {@code ThreadLocal}
 * se limpie al salir del scope, evitando fugas de memoria en pools de hilos.</p>
 *
 * <p>Si el wrapper JDBC se ejecuta fuera de un {@code CaptureContext} (por
 * ejemplo en código legacy sin anotar), {@link #currentQueryId()} devuelve
 * {@code null} y el wrapper omite la captura.</p>
 */
public final class CaptureContext implements AutoCloseable {

    private static final ThreadLocal<String> CURRENT_QUERY_ID = new ThreadLocal<>();

    private CaptureContext(String queryId) {
        CURRENT_QUERY_ID.set(queryId);
    }

    /**
     * Abre un contexto de captura para el {@code queryId} dado y lo asocia
     * al hilo actual. Debe usarse siempre en un bloque {@code try-with-resources}.
     *
     * @param queryId identificador declarado en {@code @SqlQuery#queryId}
     * @return el contexto abierto (se cierra automáticamente al salir del try)
     */
    public static CaptureContext begin(String queryId) {
        return new CaptureContext(queryId);
    }

    /**
     * Devuelve el {@code queryId} activo en el hilo actual, o {@code null}
     * si no hay ningún {@link CaptureContext} abierto.
     */
    public static String currentQueryId() {
        return CURRENT_QUERY_ID.get();
    }

    /**
     * Elimina el {@code queryId} del {@link ThreadLocal} del hilo actual.
     * Llamado automáticamente por {@code try-with-resources}.
     */
    @Override
    public void close() {
        CURRENT_QUERY_ID.remove();
    }
}
