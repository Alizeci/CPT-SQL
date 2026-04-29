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
import java.util.Map;
import java.util.logging.Logger;

/**
 * Manages the reference baseline for the benchmark engine (Phase 3).
 *
 * <p>The baseline is the set of {@link BenchmarkResult.QueryResult} metrics from the
 * last approved run, persisted in {@code baseline.json} at the project root.
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
     * Returns an empty map if {@code baseline.json} does not yet exist (first run).
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
     */
    public void save(BenchmarkResult result) {
        saveAs(result, baselinePath);
    }

    public void saveAs(BenchmarkResult result, String path) {
        BaselineFile baseline = new BaselineFile(
                result.getCommitSha(),
                result.getProfileName(),
                result.getExecutedAt().toString(),
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
            Map<String, BenchmarkResult.QueryResult> queries) {}
}
