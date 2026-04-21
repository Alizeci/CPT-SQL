package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.TransactionRecord;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Aggregates {@link TransactionRecord}s from {@link MetricsBuffer} and builds
 * the {@link LoadProfile} consumed by phase 3.
 *
 * <h3>Statistics computed per query</h3>
 * <ul>
 *   <li><b>sampleCount</b>, <b>meanMs</b>, <b>medianMs</b> (p50)</li>
 *   <li><b>p95Ms</b>, <b>p99Ms</b> — nearest-rank percentiles</li>
 *   <li><b>minMs</b>, <b>maxMs</b></li>
 *   <li><b>callsPerMinute</b> — estimated over the observation window</li>
 *   <li><b>capturedSql</b> — first non-blank SQL found in the sample set</li>
 *   <li><b>sanitizedRealData</b> — up to 10 % of samples with sanitized row data</li>
 * </ul>
 *
 * <p>The observation window is the span between the earliest and latest
 * {@code timestamp} in the sample set.</p>
 */
@Component
public class LoadProfileBuilder {

    private final MetricsBuffer metricsBuffer;

    public LoadProfileBuilder(MetricsBuffer metricsBuffer) {
        this.metricsBuffer = metricsBuffer;
    }

    /**
     * Drains {@link MetricsBuffer} and builds the {@link LoadProfile} from all
     * accumulated records. Forces a flush of any records still pending in the
     * buffer queue before draining, ensuring no samples are lost due to the
     * periodic flush interval.
     *
     * @return load profile; may contain zero entries if no samples are available
     */
    public LoadProfile build() {
        metricsBuffer.forceFlush();
        return buildFrom(metricsBuffer.drainFlushed());
    }

    /**
     * Builds the profile from a provided list of records.
     * Useful for tests and phase 3 integration.
     *
     * @param records input records
     * @return computed load profile
     */
    public LoadProfile buildFrom(List<TransactionRecord> records) {
        if (records.isEmpty()) {
            return LoadProfile.builder()
                    .generatedAt(Instant.now())
                    .totalSamples(0)
                    .queries(Collections.emptyMap())
                    .build();
        }

        Instant windowStart = records.stream()
                .map(TransactionRecord::getTimestamp)
                .min(Instant::compareTo)
                .orElse(Instant.now());
        Instant windowEnd = records.stream()
                .map(TransactionRecord::getTimestamp)
                .max(Instant::compareTo)
                .orElse(Instant.now());

        long windowMs     = Math.max(windowEnd.toEpochMilli() - windowStart.toEpochMilli(), 1L);
        double windowMins = windowMs / 60_000.0;

        Map<String, List<TransactionRecord>> byQuery = records.stream()
                .collect(Collectors.groupingBy(TransactionRecord::getQueryId));

        Map<String, LoadProfile.QueryStats> statsMap = new HashMap<>();
        for (Map.Entry<String, List<TransactionRecord>> entry : byQuery.entrySet()) {
            statsMap.put(entry.getKey(), computeStats(entry.getKey(), entry.getValue(), windowMins));
        }

        return LoadProfile.builder()
                .generatedAt(Instant.now())
                .totalSamples(records.size())
                .queries(Collections.unmodifiableMap(statsMap))
                .build();
    }

    // Statistics

    private LoadProfile.QueryStats computeStats(String queryId,
                                                List<TransactionRecord> samples,
                                                double windowMins) {
        List<Long> latencies = new ArrayList<>();
        for (TransactionRecord r : samples) {
            latencies.add(r.getLatencyMs());
        }
        Collections.sort(latencies);

        long   n      = latencies.size();
        double mean   = latencies.stream().mapToLong(Long::longValue).average().orElse(0.0);
        double median = percentile(latencies, 50.0);
        double p95    = percentile(latencies, 95.0);
        double p99    = percentile(latencies, 99.0);
        double cpm    = n / windowMins;
        long   min    = latencies.get(0);
        long   max    = latencies.get(latencies.size() - 1);

        String capturedSql = samples.stream()
                .map(TransactionRecord::getSql)
                .filter(s -> s != null && !s.isBlank())
                .findFirst()
                .orElse(null);

        double avgRowCount = samples.stream()
                .mapToLong(TransactionRecord::getRowCount)
                .filter(rc -> rc > 0)
                .average()
                .orElse(0.0);

        int maxRealRows = Math.max(1, (int) Math.ceil(n * 0.10));
        List<Map<String, Object>> sanitizedRealData = samples.stream()
                .filter(r -> r.getSanitizedData() != null && !r.getSanitizedData().isEmpty())
                .limit(maxRealRows)
                .map(TransactionRecord::getSanitizedData)
                .collect(Collectors.toList());

        return LoadProfile.QueryStats.builder()
                .queryId(queryId)
                .sampleCount(n)
                .meanMs(mean)
                .medianMs(median)
                .p95Ms(p95)
                .p99Ms(p99)
                .callsPerMinute(cpm)
                .minMs(min)
                .maxMs(max)
                .capturedSql(capturedSql)
                .avgRowCount(avgRowCount)
                .sanitizedRealData(sanitizedRealData.isEmpty() ? null : sanitizedRealData)
                .build();
    }

    /**
     * Computes percentile {@code p} over a sorted latency list using nearest-rank.
     */
    private double percentile(List<Long> sorted, double p) {
        if (sorted.isEmpty()) return 0.0;
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        index = Math.max(0, Math.min(index, sorted.size() - 1));
        return sorted.get(index);
    }
}
