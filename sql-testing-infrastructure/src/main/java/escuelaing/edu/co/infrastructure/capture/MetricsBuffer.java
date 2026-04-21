package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.TransactionRecord;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
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
 *   <li>Both the incoming queue and the {@code flushed} accumulation list apply a
 *       drop-on-full policy: records are discarded (never block the caller) when
 *       either limit is reached.</li>
 *   <li>Flushed records accumulate until {@link LoadProfileBuilder} consumes them
 *       via {@link #drainFlushed()}.</li>
 * </ul>
 *
 * <h3>Configuration (application.properties)</h3>
 * <pre>
 * loadtest.buffer.queueCapacity=10000
 * loadtest.buffer.flushedCapacity=50000
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

    @Value("${loadtest.buffer.flushedCapacity:50000}")
    private int flushedCapacity;

    private BlockingQueue<TransactionRecord> queue;

    // Guarded by synchronized (flushed) on every compound access.
    private final List<TransactionRecord> flushed = new ArrayList<>();

    private volatile boolean running = false;
    private Thread flusherThread;

    @PostConstruct
    public void start() {
        queue = new LinkedBlockingQueue<>(queueCapacity);
        running = true;
        flusherThread = new Thread(this::flushLoop, "metrics-flusher");
        flusherThread.setDaemon(true);
        flusherThread.start();
        LOG.info("[MetricsBuffer] Flusher thread started (queueCapacity=" + queueCapacity
                + ", flushedCapacity=" + flushedCapacity
                + ", flushIntervalMs=" + flushIntervalMs + ", batchSize=" + batchSize + ").");
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (flusherThread != null) {
            flusherThread.interrupt();
        }
        forceFlush();
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
     * Immediately moves all records still in the queue into the {@code flushed} list,
     * bypassing the flusher thread interval. Call this before {@link #drainFlushed()}
     * to guarantee no records are lost due to the periodic flush lag.
     */
    public void forceFlush() {
        List<TransactionRecord> batch = new ArrayList<>();
        queue.drainTo(batch);
        if (batch.isEmpty()) return;
        synchronized (flushed) {
            int available = flushedCapacity - flushed.size();
            if (available <= 0) {
                LOG.warning("[MetricsBuffer] forceFlush: flushed list at capacity — "
                        + batch.size() + " records dropped.");
                return;
            }
            List<TransactionRecord> accepted = batch.subList(0, Math.min(batch.size(), available));
            flushed.addAll(accepted);
            int dropped = batch.size() - accepted.size();
            if (dropped > 0) {
                LOG.warning("[MetricsBuffer] forceFlush: " + dropped + " records dropped.");
            }
            LOG.info("[MetricsBuffer] forceFlush: " + accepted.size() + " records moved to flushed.");
        }
    }

    /**
     * Drains and returns all records currently in {@code flushed}.
     * Call {@link #forceFlush()} first to include records still pending in the queue.
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
        if (batch.isEmpty()) return;

        synchronized (flushed) {
            int available = flushedCapacity - flushed.size();
            if (available <= 0) {
                LOG.warning("[MetricsBuffer] Flushed list at capacity (" + flushedCapacity
                        + ") — " + batch.size() + " records dropped.");
                return;
            }
            List<TransactionRecord> accepted = batch.subList(0, Math.min(batch.size(), available));
            flushed.addAll(accepted);
            int dropped = batch.size() - accepted.size();
            if (dropped > 0) {
                LOG.warning("[MetricsBuffer] Flushed list nearly full — " + dropped + " records dropped.");
            }
            LOG.fine("[MetricsBuffer] Flushed " + accepted.size() + " records. Total accumulated: " + flushed.size());
        }
    }
}
