package escuelaing.edu.co.infrastructure.capture;

import escuelaing.edu.co.domain.model.LoadProfile;

import java.util.Map;
import java.util.logging.Logger;

/**
 * Merges the SQL captured from a PR run into the load profile from the capture environment.
 *
 * <p>Only {@code capturedSql} is replaced per {@code queryId}; the latency statistics and
 * the protected statistics of the base profile are kept, so the benchmark runs the PR's
 * query under the captured load.</p>
 */
public class LoadProfileMerger {

    private static final Logger LOG = Logger.getLogger(LoadProfileMerger.class.getName());

    private LoadProfileMerger() {}

    /**
     * Updates {@code capturedSql} in {@code base} in place with non-blank values from
     * {@code pr}, matched by {@code queryId}. Queries present in only one profile are logged
     * and skipped; all other fields of {@code base} are unchanged.
     *
     * @param base load profile from the capture environment (statistics source; mutated)
     * @param pr   profile captured from the PR run (SQL source)
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
