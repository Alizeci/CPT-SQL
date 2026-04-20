package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.LoadProfile;

import java.util.Map;
import java.util.logging.Logger;

/**
 * Merges captured SQL from a PR run into a production load profile.
 *
 * <p>The production profile's latency statistics (p95Ms, meanMs, callsPerMinute,
 * etc.) are preserved unchanged. Only {@code capturedSql} is updated per
 * {@code queryId} from the PR profile, allowing the benchmark to execute the
 * PR's query under real production load conditions.</p>
 */
public class LoadProfileMerger {

    private static final Logger LOG = Logger.getLogger(LoadProfileMerger.class.getName());

    private LoadProfileMerger() {}

    /**
     * Updates {@code capturedSql} in {@code base} with values from {@code pr},
     * matched by {@code queryId}. All other fields in {@code base} are unchanged.
     *
     * @param base production profile (stats source)
     * @param pr   PR profile (SQL source)
     * @return number of queries whose capturedSql was updated
     */
    public static int merge(LoadProfile base, LoadProfile pr) {
        Map<String, LoadProfile.QueryStats> baseQueries = base.getQueries();
        Map<String, LoadProfile.QueryStats> prQueries   = pr.getQueries();

        int updated = 0;
        for (Map.Entry<String, LoadProfile.QueryStats> entry : baseQueries.entrySet()) {
            String queryId = entry.getKey();
            LoadProfile.QueryStats prStats = prQueries.get(queryId);

            if (prStats == null) {
                LOG.warning("[LoadProfileMerger] queryId '" + queryId
                        + "' not found in PR profile — keeping existing capturedSql.");
                continue;
            }

            if (prStats.getCapturedSql() != null && !prStats.getCapturedSql().isBlank()) {
                entry.getValue().setCapturedSql(prStats.getCapturedSql());
                updated++;
            } else {
                LOG.warning("[LoadProfileMerger] queryId '" + queryId
                        + "' has no capturedSql in PR profile — keeping existing capturedSql.");
            }
        }

        for (String queryId : prQueries.keySet()) {
            if (!baseQueries.containsKey(queryId)) {
                LOG.warning("[LoadProfileMerger] queryId '" + queryId
                        + "' in PR profile not found in production profile — skipped.");
            }
        }

        return updated;
    }
}
