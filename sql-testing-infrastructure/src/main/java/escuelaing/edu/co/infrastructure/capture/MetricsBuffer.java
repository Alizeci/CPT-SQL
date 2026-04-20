package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.TransactionRecord;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * In-memory buffer for {@link TransactionRecord}s captured by the JDBC wrapper.
 *
 * <h3>Design</h3>
 * <ul>
 *   <li>Uses a {@link LinkedBlockingQueue} with a configurable maximum capacity
 *       to bound memory usage under load spikes.</li>
 *   <li>A dedicated flusher thread ({@code metrics-flusher}) drains the queue in
 *       batches every {@code loadtest.buffer.flushIntervalMs} ms, minimising
 *       contention with application threads.</li>
 *   <li>If the queue is full, the incoming record is dropped (drop-on-full policy)
 *       to avoid blocking application threads.</li>
 *   <li>Flushed records accumulate in {@code flushed} (a {@code synchronized} list)
 *       until {@link LoadProfileBuilder} consumes them.</li>
 * </ul>
 *
 * <h3>Configuration (application.properties)</h3>
 * <pre>
 * loadtest.buffer.queueCapacity=10000
 * loadtest.buffer.flushIntervalMs=500
 * loadtest.buffer.batchSize=200
 * </pre>
 */
@Component
public class MetricsBuffer {

    private static final Logger LOG = Logger.getLogger(MetricsBuffer.class.getName());

    @Value("${loadtest.buffer.queueCapacity:10000}")
    private int queueCapacity;

    @Value("${loadtest.buffer.flushIntervalMs:500}")
    private long flushIntervalMs;

    @Value("${loadtest.buffer.batchSize:200}")
    private int batchSize;

    private BlockingQueue<TransactionRecord> queue;

    private final List<TransactionRecord> flushed =
            Collections.synchronizedList(new ArrayList<>());

    private volatile boolean running = false;
    private Thread flusherThread;

    @PostConstruct
    public void start() {
        queue = new LinkedBlockingQueue<>(queueCapacity);
        running = true;
        flusherThread = new Thread(this::flushLoop, "metrics-flusher");
        flusherThread.setDaemon(true);
        flusherThread.start();
        LOG.info("[MetricsBuffer] Flusher thread started (capacity=" + queueCapacity
                + ", flushIntervalMs=" + flushIntervalMs + ", batchSize=" + batchSize + ").");
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (flusherThread != null) {
            flusherThread.interrupt();
        }
        drainBatch();
        LOG.info("[MetricsBuffer] Stopped. Total records in flushed: " + flushed.size());
    }

    // Public API

    /**
     * Enqueues a {@link TransactionRecord}. Non-blocking: if the queue is full
     * the record is dropped and a warning is logged.
     *
     * @param record record to enqueue
     */
    public void record(TransactionRecord record) {
        boolean accepted = queue.offer(record);
        if (!accepted) {
            LOG.warning("[MetricsBuffer] Queue full — record dropped for queryId=" + record.getQueryId());
        }
    }

    /**
     * Drains and returns all records currently in {@code flushed}.
     * Called by {@link LoadProfileBuilder} to build the load profile.
     *
     * @return list of records (may be empty)
     */
    public List<TransactionRecord> drainFlushed() {
        synchronized (flushed) {
            List<TransactionRecord> snapshot = new ArrayList<>(flushed);
            flushed.clear();
            return snapshot;
        }
    }

    /** Number of records currently in the capture queue (pending flush). */
    public int pendingCount() {
        return queue.size();
    }

    /** Number of records already flushed and available to LoadProfileBuilder. */
    public int flushedCount() {
        return flushed.size();
    }

    private void flushLoop() {
        while (running) {
            try {
                TimeUnit.MILLISECONDS.sleep(flushIntervalMs);
                drainBatch();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void drainBatch() {
        List<TransactionRecord> batch = new ArrayList<>(batchSize);
        queue.drainTo(batch, batchSize);
        if (!batch.isEmpty()) {
            flushed.addAll(batch);
            LOG.fine("[MetricsBuffer] Flushed " + batch.size() + " records. Total accumulated: " + flushed.size());
        }
    }
}
