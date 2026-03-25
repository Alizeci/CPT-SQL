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
 * Agrega los {@link TransactionRecord}s capturados y construye el
 * {@link LoadProfile} que servirá como insumo para la Fase 3.
 *
 * <h3>Estadísticas calculadas por consulta</h3>
 * <ul>
 *   <li><b>sampleCount</b> — número de muestras.</li>
 *   <li><b>meanMs</b> — latencia media aritmética.</li>
 *   <li><b>medianMs</b> — percentil 50 (p50).</li>
 *   <li><b>p95Ms</b> — percentil 95 (nearest-rank).</li>
 *   <li><b>p99Ms</b> — percentil 99 (nearest-rank).</li>
 *   <li><b>callsPerMinute</b> — frecuencia estimada sobre la ventana de observación.</li>
 * </ul>
 *
 * <p>La ventana de observación se calcula como la diferencia entre el
 * {@code timestamp} más antiguo y el más reciente en el conjunto de muestras.</p>
 */
@Component
public class LoadProfileBuilder {

    private final MetricsBuffer metricsBuffer;

    public LoadProfileBuilder(MetricsBuffer metricsBuffer) {
        this.metricsBuffer = metricsBuffer;
    }

    /**
     * Drena el {@link MetricsBuffer} y construye el {@link LoadProfile} con
     * todos los registros acumulados hasta ahora.
     *
     * @return perfil de carga; puede contener cero entradas si no hay muestras.
     */
    public LoadProfile build() {
        List<TransactionRecord> records = metricsBuffer.drainFlushed();
        return buildFrom(records);
    }

    /**
     * Construye el perfil a partir de una lista de registros ya recolectados.
     * Útil para tests y para la integración con la Fase 3.
     *
     * @param records registros de entrada
     * @return perfil de carga calculado
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

    // -------------------------------------------------------------------------
    // Cálculo estadístico
    // -------------------------------------------------------------------------

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

        // avgRowCount: promedio de filas afectadas (sólo muestras con rowCount > 0, i.e. writes)
        double avgRowCount = samples.stream()
                .mapToLong(TransactionRecord::getRowCount)
                .filter(rc -> rc > 0)
                .average()
                .orElse(0.0);

        // sanitizedRealData: máx. 10 % de las muestras — filas reales sanitizadas
        // capturadas por JdbcWrapper vía ResultSetCaptureHandler.
        // Si ninguna muestra tiene datos (queries de escritura, p. ej.) la lista queda vacía.
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
     * Calcula el percentil {@code p} sobre una lista de latencias ya ordenada.
     * Usa el método <i>nearest rank</i>.
     */
    private double percentile(List<Long> sorted, double p) {
        if (sorted.isEmpty()) return 0.0;
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        index = Math.max(0, Math.min(index, sorted.size() - 1));
        return sorted.get(index);
    }
}
