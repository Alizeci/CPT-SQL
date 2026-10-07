package escuelaing.edu.co.infrastructure.analysis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import escuelaing.edu.co.domain.model.BenchmarkResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Reads and writes the baseline used by the gate.
 *
 * <p>The baseline is the set of {@link BenchmarkResult.QueryResult} metrics from the
 * last approved run, persisted in {@code baseline.json} (path relative to the working
 * directory) together with the conditions it was measured under.
 * {@link DegradationDetector} uses it to detect {@code BASELINE_EXCEEDED} and
 * {@code PLAN_CHANGED} degradations.</p>
 *
 * <p>Each save overwrites the previous {@code baseline.json}. History is preserved
 * in git — any prior version can be retrieved with
 * {@code git show HEAD~n:baseline.json}.</p>
 *
 * <pre>
 * loadtest.baseline.path=baseline.json
 * </pre>
 */
@Component
public class BaselineManager {

    private static final Logger LOG = Logger.getLogger(BaselineManager.class.getName());

    @Value("${loadtest.baseline.path:baseline.json}")
    private String baselinePath;

    private final ObjectMapper mapper;

    public BaselineManager() {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    // Public API

    /**
     * Reads the stored baseline and returns a {@code queryId → QueryResult} map.
     * Returns an empty map if {@code baseline.json} does not exist or cannot be read.
     *
     * @return immutable map with the current baseline
     */
    public Map<String, BenchmarkResult.QueryResult> load() {
        File file = new File(baselinePath);
        if (!file.exists()) {
            LOG.info("[BaselineManager] baseline.json not found — first run.");
            return Collections.emptyMap();
        }
        try {
            BaselineFile baseline = mapper.readValue(file, BaselineFile.class);
            return baseline.queries() != null
                    ? Collections.unmodifiableMap(baseline.queries())
                    : Collections.emptyMap();
        } catch (IOException e) {
            LOG.warning("[BaselineManager] Could not read baseline.json: " + e.getMessage());
            return Collections.emptyMap();
        }
    }

    /**
     * Persists {@code result} as the new baseline in {@code baseline.json}.
     * Should only be called when the benchmark produces a PASS verdict.
     *
     * @param result approved result that becomes the new baseline
     * @throws RuntimeException if the file cannot be written
     */
    public void save(BenchmarkResult result) {
        saveAs(result, baselinePath);
    }

    /** Same as {@link #save} but writes to {@code path} instead of the configured location. */
    public void saveAs(BenchmarkResult result, String path) {
        BaselineFile baseline = new BaselineFile(
                result.getCommitSha(),
                result.getProfileName(),
                result.getExecutedAt().toString(),
                result.getTestProfileName(),
                result.getRowsPerTable(),
                result.getLoadProfileFingerprint(),
                new HashMap<>(result.getQueries()));

        try {
            mapper.writerWithDefaultPrettyPrinter().writeValue(new File(path), baseline);
            LOG.info("[BaselineManager] " + path + " updated (commit=" + result.getCommitSha() + ").");
        } catch (IOException e) {
            throw new RuntimeException("Could not save " + path, e);
        }
    }

    // Serialization model

    /** Internal structure of {@code baseline.json}. */
    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
    public record BaselineFile(
            String commitSha,
            String profileName,
            String savedAt,
            /** Test profile used to measure the baseline (e.g. "light"). */
            String testProfileName,
            /** Mirror rows per table used to measure the baseline. */
            long rowsPerTable,
            /** Fingerprint of the load-profile inputs used to generate the mirror. */
            String loadProfileFingerprint,
            Map<String, BenchmarkResult.QueryResult> queries) {}

    /**
     * Checks that the stored baseline was measured under the same conditions as
     * {@code current}: same test profile, same mirror volume and same load-profile inputs.
     * Comparing across conditions produces spurious or masked degradations (e.g. plan
     * cost changes caused by different synthetic data, not by the code under review).
     * A baseline without this provenance metadata, or one that cannot be read, is
     * incompatible.
     *
     * @return {@code "MISSING"}, {@code "COMPARABLE"} or {@code "INCOMPATIBLE: <reasons>"}
     */
    public String compatibilityWith(BenchmarkResult current) {
        File file = new File(baselinePath);
        if (!file.exists()) return "MISSING";
        BaselineFile b;
        try {
            b = mapper.readValue(file, BaselineFile.class);
        } catch (IOException e) {
            return "INCOMPATIBLE: unreadable baseline (" + e.getMessage() + ")";
        }
        List<String> reasons = new ArrayList<>();
        if (b.testProfileName() == null || b.loadProfileFingerprint() == null || b.rowsPerTable() == 0) {
            reasons.add("baseline has no provenance metadata (generated by an older version)");
        } else {
            if (!b.testProfileName().equals(current.getTestProfileName())) {
                reasons.add("testProfile " + b.testProfileName() + " != " + current.getTestProfileName());
            }
            if (b.rowsPerTable() != current.getRowsPerTable()) {
                reasons.add("rowsPerTable " + b.rowsPerTable() + " != " + current.getRowsPerTable());
            }
            if (!b.loadProfileFingerprint().equals(current.getLoadProfileFingerprint())) {
                reasons.add("load profile changed since the baseline was measured");
            }
        }
        return reasons.isEmpty() ? "COMPARABLE" : "INCOMPATIBLE: " + String.join("; ", reasons);
    }
}
