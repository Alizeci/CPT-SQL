package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.DpRelease;
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
 * Aggregates {@link TransactionRecord}s from {@link MetricsBuffer} into the
 * {@link LoadProfile} that the benchmark uses to drive the mirror database.
 *
 * <h3>Statistics computed per query</h3>
 * <ul>
 *   <li><b>sampleCount</b>, <b>meanMs</b>, <b>medianMs</b> (p50)</li>
 *   <li><b>p95Ms</b>, <b>p99Ms</b> — nearest-rank percentiles</li>
 *   <li><b>minMs</b>, <b>maxMs</b></li>
 *   <li><b>callsPerMinute</b> — samples per minute over the observation window (sampled
 *       executions, not total traffic)</li>
 *   <li><b>avgRowCount</b> — mean of the positive update counts; 0 if none</li>
 *   <li><b>capturedSql</b> — first non-blank SQL found in the sample set</li>
 *   <li><b>sanitizedRealData</b> — only when {@code CPT_DP_KEEP_SANITIZED_ROWS=true}
 *       (debugging, no formal guarantee); {@code null} by default</li>
 * </ul>
 *
 * <p>The profile's {@link DpRelease} is the table-level release from {@link TableProfiler}
 * when one is attached, otherwise a row-level release of the captured sanitized rows
 * ({@link DpReleaseBuilder}); foreign-key concentration curves are merged in when attached.
 * Apart from the debugging option above, sanitized rows are discarded after the release, so
 * the profile contains no production record.</p>
 *
 * <p>The observation window is the span between the earliest and latest
 * {@code timestamp} in the sample set (at least 1 ms).</p>
 *
 * <p>Not thread-safe: the {@code with*} methods mutate this instance (a singleton bean), and
 * attached releases apply to every later build.</p>
 */
@Component
public class LoadProfileBuilder {

    private final MetricsBuffer metricsBuffer;
    private DpConfig dpConfig;
    private Map<String, DpRelease.ForeignKeyDegree> foreignKeyRelease = Collections.emptyMap();
    private DpRelease tableRelease;

    @org.springframework.beans.factory.annotation.Autowired
    public LoadProfileBuilder(MetricsBuffer metricsBuffer) {
        this(metricsBuffer, DpConfig.fromEnv());
    }

    public LoadProfileBuilder(MetricsBuffer metricsBuffer, DpConfig dpConfig) {
        this.metricsBuffer = metricsBuffer;
        this.dpConfig      = dpConfig;
    }

    /** Overrides the DP configuration (defaults to {@link DpConfig#fromEnv()}). */
    public LoadProfileBuilder withDpConfig(DpConfig dpConfig) {
        this.dpConfig = dpConfig;
        return this;
    }

    /**
     * Attaches the foreign-key concentration curves computed by {@link FkDegreeProfiler}
     * against the capture database; they are merged into the profile's {@link DpRelease}.
     */
    public LoadProfileBuilder withForeignKeyRelease(Map<String, DpRelease.ForeignKeyDegree> release) {
        this.foreignKeyRelease = release != null ? release : Collections.emptyMap();
        return this;
    }

    /**
     * Uses a table-level release computed by {@link TableProfiler} over the whole capture
     * tables instead of the row-level release over the rows the application read.
     */
    public LoadProfileBuilder withTableRelease(DpRelease release) {
        this.tableRelease = release;
        return this;
    }

    /**
     * Flushes records still pending in {@link MetricsBuffer}, drains it and builds the
     * profile from them.
     *
     * @return load profile; has no query entries if no samples are available
     */
    public LoadProfile build() {
        metricsBuffer.forceFlush();
        return buildFrom(metricsBuffer.drainFlushed());
    }

    /**
     * Builds the profile from the given records without touching {@link MetricsBuffer}.
     * With no records, returns an empty profile without a {@link DpRelease}.
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

        List<Map<String, Object>> allSanitized = records.stream()
                .map(TransactionRecord::getSanitizedData)
                .filter(d -> d != null && !d.isEmpty())
                .collect(Collectors.toList());
        DpRelease dpRelease = tableRelease != null
                ? tableRelease
                : allSanitized.isEmpty() ? null : new DpReleaseBuilder(dpConfig).build(allSanitized);
        if (!foreignKeyRelease.isEmpty()) {
            if (dpRelease == null) {
                dpRelease = DpRelease.builder()
                        .mechanism(DpReleaseBuilder.MECHANISM)
                        .delta(dpConfig.delta())
                        .numeric(Collections.emptyMap())
                        .categorical(Collections.emptyMap())
                        .suppressedColumns(List.of())
                        .build();
            }
            dpRelease.setMechanism(dpRelease.getMechanism() + "; " + FkDegreeProfiler.MECHANISM);
            dpRelease.setEpsilonForeignKey(dpConfig.epsilonForeignKey());
            dpRelease.setDeltaForeignKey(dpConfig.delta());
            dpRelease.setForeignKeys(new java.util.TreeMap<>(foreignKeyRelease));
        }

        return LoadProfile.builder()
                .generatedAt(Instant.now())
                .totalSamples(records.size())
                .queries(Collections.unmodifiableMap(statsMap))
                .dpRelease(dpRelease)
                .build();
    }

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

        // Raw sanitized rows are kept only on explicit request (debugging, no formal guarantee).
        List<Map<String, Object>> sanitizedRealData = !dpConfig.keepSanitizedRows()
                ? List.of()
                : samples.stream()
                        .filter(r -> r.getSanitizedData() != null && !r.getSanitizedData().isEmpty())
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

    /** Nearest-rank percentile {@code p} (0–100) of an ascending list; 0 if empty. */
    private double percentile(List<Long> sorted, double p) {
        if (sorted.isEmpty()) return 0.0;
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        index = Math.max(0, Math.min(index, sorted.size() - 1));
        return sorted.get(index);
    }
}
