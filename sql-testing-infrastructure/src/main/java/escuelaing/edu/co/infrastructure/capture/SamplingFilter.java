package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.QueryEntry;
import escuelaing.edu.co.infrastructure.analysis.QueryRegistryLoader;
import org.springframework.stereotype.Component;

import java.util.Random;

/**
 * Decides whether a query execution should be recorded as a
 * {@link escuelaing.edu.co.domain.model.TransactionRecord}.
 *
 * <h3>Sampling rules (evaluated in order)</h3>
 * <ol>
 *   <li>{@code priority == HIGH} → always record.</li>
 *   <li>Latency exceeds {@code maxResponseTimeMs} declared in {@code @Req}
 *       → always record (performance anomaly).</li>
 *   <li>None of the above → record with 10 % probability.</li>
 * </ol>
 *
 * <p>If the {@code queryId} is not in the registry (legacy code without
 * {@code @SqlQuery}), only the probabilistic rule applies.</p>
 */
@Component
public class SamplingFilter {

    private static final double DEFAULT_SAMPLE_RATE = 0.10;

    private final QueryRegistryLoader registryLoader;
    private final Random random;

    public SamplingFilter(QueryRegistryLoader registryLoader) {
        this.registryLoader = registryLoader;
        this.random = new Random();
    }

    /**
     * Returns {@code true} if the execution should be recorded.
     *
     * @param queryId   query identifier from {@link CaptureContext}; {@code null} skips capture
     * @param latencyMs latency measured by the JDBC wrapper, in milliseconds
     */
    public boolean shouldRecord(String queryId, long latencyMs) {
        if (queryId == null) {
            return false;
        }

        QueryEntry entry = registryLoader.get(queryId);

        if (entry != null) {
            if ("HIGH".equalsIgnoreCase(entry.getPriority())) {
                return true;
            }
            if (entry.isHasReq() && latencyMs > entry.getMaxResponseTimeMs()) {
                return true;
            }
        }

        return random.nextDouble() < DEFAULT_SAMPLE_RATE;
    }
}
