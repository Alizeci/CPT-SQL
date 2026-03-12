package escuelaing.edu.co.infrastructure;

import escuelaing.edu.co.domain.model.TransactionRecord;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Buffer en memoria para {@link TransactionRecord}s capturados por el wrapper JDBC.
 *
 * <h3>Diseño</h3>
 * <ul>
 *   <li>Usa un {@link LinkedBlockingQueue} con capacidad máxima de 10 000 registros
 *       para acotar el uso de memoria en picos de carga.</li>
 *   <li>Un hilo de escritura dedicado ({@code metrics-flusher}) vacía la cola en
 *       lotes cada {@value #FLUSH_INTERVAL_MS} ms, minimizando la contención con
 *       los hilos de aplicación.</li>
 *   <li>Si la cola está llena, el nuevo registro se descarta (política
 *       <i>drop-on-full</i>) para no bloquear a los hilos de aplicación.</li>
 *   <li>Los registros descargados se acumulan en {@code flushed} (lista protegida
 *       con {@code synchronized}) hasta que {@link LoadProfileBuilder} los consume.</li>
 * </ul>
 */
@Component
public class MetricsBuffer {

    private static final Logger LOG = Logger.getLogger(MetricsBuffer.class.getName());

    private static final int    QUEUE_CAPACITY    = 10_000;
    private static final long   FLUSH_INTERVAL_MS = 500L;
    private static final int    BATCH_SIZE        = 200;

    private final BlockingQueue<TransactionRecord> queue =
            new LinkedBlockingQueue<>(QUEUE_CAPACITY);

    private final List<TransactionRecord> flushed =
            Collections.synchronizedList(new ArrayList<>());

    private volatile boolean running = false;
    private Thread flusherThread;

    // -------------------------------------------------------------------------
    // Ciclo de vida del bean
    // -------------------------------------------------------------------------

    @PostConstruct
    public void start() {
        running = true;
        flusherThread = new Thread(this::flushLoop, "metrics-flusher");
        flusherThread.setDaemon(true);
        flusherThread.start();
        LOG.info("[MetricsBuffer] Hilo de escritura iniciado.");
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (flusherThread != null) {
            flusherThread.interrupt();
        }
        // Último flush antes de apagar
        drainBatch();
        LOG.info("[MetricsBuffer] Detenido. Total registros en flushed: " + flushed.size());
    }

    // -------------------------------------------------------------------------
    // API pública
    // -------------------------------------------------------------------------

    /**
     * Encola un {@link TransactionRecord}. No bloquea: si la cola está llena
     * el registro se descarta y se registra una advertencia.
     *
     * @param record registro a encolar
     */
    public void record(TransactionRecord record) {
        boolean accepted = queue.offer(record);
        if (!accepted) {
            LOG.warning("[MetricsBuffer] Cola llena — registro descartado para queryId="
                    + record.getQueryId());
        }
    }

    /**
     * Devuelve y elimina todos los registros actualmente en {@code flushed}.
     * Llamado por {@link LoadProfileBuilder} para construir el perfil de carga.
     *
     * @return lista de registros (puede estar vacía)
     */
    public List<TransactionRecord> drainFlushed() {
        synchronized (flushed) {
            List<TransactionRecord> snapshot = new ArrayList<>(flushed);
            flushed.clear();
            return snapshot;
        }
    }

    /** Número de registros actualmente en la cola de captura (pendientes de flush). */
    public int pendingCount() {
        return queue.size();
    }

    /** Número de registros ya descargados y disponibles para el LoadProfileBuilder. */
    public int flushedCount() {
        return flushed.size();
    }

    // -------------------------------------------------------------------------
    // Lógica interna del hilo de escritura
    // -------------------------------------------------------------------------

    private void flushLoop() {
        while (running) {
            try {
                TimeUnit.MILLISECONDS.sleep(FLUSH_INTERVAL_MS);
                drainBatch();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void drainBatch() {
        List<TransactionRecord> batch = new ArrayList<>(BATCH_SIZE);
        queue.drainTo(batch, BATCH_SIZE);
        if (!batch.isEmpty()) {
            flushed.addAll(batch);
            LOG.fine("[MetricsBuffer] Flush: " + batch.size() + " registros. Total acumulado: " + flushed.size());
        }
    }
}
