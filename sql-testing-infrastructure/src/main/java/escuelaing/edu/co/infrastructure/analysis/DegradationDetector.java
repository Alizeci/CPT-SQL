package escuelaing.edu.co.infrastructure.analysis;

import escuelaing.edu.co.domain.model.BenchmarkResult;
import escuelaing.edu.co.domain.model.DegradationReport;
import escuelaing.edu.co.domain.model.QueryEntry;
import escuelaing.edu.co.infrastructure.dialect.DatabaseDialect;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Detects performance degradations by comparing a {@link BenchmarkResult}
 * against two reference sources:
 *
 * <ol>
 *   <li><b>{@code @Req} contracts:</b> {@code maxResponseTimeMs} and
 *       {@code allowPlanChange} declared in source code and loaded from
 *       {@code queries.json} via {@link QueryRegistryLoader}.</li>
 *   <li><b>Baseline:</b> per-query metrics stored in {@code baseline.json} by
 *       {@link BaselineManager}. Used only when the baseline is {@code COMPARABLE}
 *       (see {@link BaselineManager#compatibilityWith}); otherwise the relative
 *       criteria are skipped.</li>
 * </ol>
 *
 * <p>Evaluation order per query:</p>
 * <ol>
 *   <li>{@code P95_EXCEEDED} — measured p95 &gt; {@code maxResponseTimeMs}.</li>
 *   <li>{@code SLO_PROXIMITY} — measured p95 is within the SLA but above
 *       {@code sloProximityPct} of it (default 80 %).</li>
 *   <li>{@code PLAN_CHANGED} — only when {@code allowPlanChange=false}: plan cost
 *       &gt; baseline cost × (1 + {@code planCostTolerancePct}) (default 20 %)
 *       <em>and</em> {@code slaRiskPct} &ge; {@code planChangeSlaRiskThresholdPct}
 *       (default 50 %). Below that threshold the plan change is reported as a
 *       {@code BASELINE_EXCEEDED} warning instead.</li>
 *   <li>{@code BASELINE_EXCEEDED} — measured p95 &gt; baseline p95 × (1 +
 *       {@code baselineTolerancePct}) (default 10 %) and at least
 *       {@code baselineMinDeltaMs} (default 10 ms) above it.</li>
 * </ol>
 *
 * <p>{@code BASELINE_EXCEEDED} warnings are dropped for queries that already have a
 * blocking finding.</p>
 */
@Component
public class DegradationDetector {

    private static final Logger LOG = Logger.getLogger(DegradationDetector.class.getName());

    @Value("${loadtest.detector.baselineTolerancePct:0.10}")
    private double baselineTolerancePct;

    /**
     * Minimum absolute p95 increase (ms) for {@code BASELINE_EXCEEDED}. Avoids flagging
     * millisecond-scale queries whose relative change is pure measurement noise
     * (e.g. 2 ms → 6 ms on a single sample).
     */
    @Value("${loadtest.detector.baselineMinDeltaMs:10}")
    private double baselineMinDeltaMs = 10;

    @Value("${loadtest.detector.planCostTolerancePct:0.20}")
    private double planCostTolerancePct;

    @Value("${loadtest.detector.sloProximityPct:0.80}")
    private double sloProximityPct;

    /**
     * Fraction of the SLA (p95 / {@code maxResponseTimeMs}) at which a plan cost increase
     * becomes a blocking {@code PLAN_CHANGED}. Below it the change is only a
     * {@code BASELINE_EXCEEDED} warning, since the query is still well within its SLA
     * budget; the nightly run at higher volume confirms the impact. Default 0.50.
     */
    @Value("${loadtest.detector.planChangeSlaRiskThresholdPct:0.50}")
    private double planChangeSlaRiskThresholdPct;

    private final QueryRegistryLoader queryRegistry;
    private final BaselineManager baselineManager;
    private final DatabaseDialect adapter;

    public DegradationDetector(QueryRegistryLoader queryRegistry,
                                BaselineManager baselineManager,
                                DatabaseDialect adapter) {
        this.queryRegistry   = queryRegistry;
        this.baselineManager = baselineManager;
        this.adapter         = adapter;
    }

    // Public API

    /**
     * Evaluates {@code result} and produces a {@link DegradationReport}.
     *
     * @param result benchmark result to evaluate
     * @return report with the detected degradations (may be empty) and the baseline status
     *         ({@code COMPARABLE}, {@code MISSING} or {@code INCOMPATIBLE: <reason>})
     */
    public DegradationReport detect(BenchmarkResult result) {
        Map<String, QueryEntry> registry = queryRegistry.getRegistry();
        // Invariant: only compare against a baseline measured under the same test profile,
        // mirror volume and load-profile inputs. Otherwise skip the baseline-relative
        // criteria and evaluate the contract criteria only.
        String baselineStatus = baselineManager.compatibilityWith(result);
        Map<String, BenchmarkResult.QueryResult> baseline;
        if ("COMPARABLE".equals(baselineStatus)) {
            baseline = baselineManager.load();
        } else {
            baseline = Map.of();
            if (!"MISSING".equals(baselineStatus)) {
                LOG.warning("[DegradationDetector] Baseline not comparable — " + baselineStatus
                        + ". BASELINE_EXCEEDED and PLAN_CHANGED skipped; regenerate the baseline "
                        + "(update-baseline workflow) before relying on relative criteria.");
            }
        }

        List<DegradationReport.Degradation> degradations = new ArrayList<>();

        for (Map.Entry<String, BenchmarkResult.QueryResult> entry
                : result.getQueries().entrySet()) {

            String queryId  = entry.getKey();
            BenchmarkResult.QueryResult current = entry.getValue();
            QueryEntry req  = registry.get(queryId);

            checkReqThreshold(queryId, current, req, degradations);
            checkSloProximity(queryId, current, req, degradations);
            checkPlanChange(queryId, current, baseline.get(queryId), req, degradations);
            checkBaselineExceeded(queryId, current, baseline.get(queryId), degradations);
        }

        // Drop BASELINE_EXCEEDED for queries that already have a blocking degradation
        // (P95_EXCEEDED, SLO_PROXIMITY or PLAN_CHANGED): the warning is redundant.
        java.util.Set<String> blockedQueries = degradations.stream()
                .filter(r -> r.getType() != DegradationReport.DegradationType.BASELINE_EXCEEDED)
                .map(DegradationReport.Degradation::getQueryId)
                .collect(java.util.stream.Collectors.toSet());

        degradations.removeIf(r -> r.getType() == DegradationReport.DegradationType.BASELINE_EXCEEDED
                && blockedQueries.contains(r.getQueryId()));

        boolean hasDegradations = !degradations.isEmpty();
        LOG.info("[DegradationDetector] " + (hasDegradations
                ? degradations.size() + " degradation(s) detected."
                : "No degradations detected."));

        return DegradationReport.builder()
                .generatedAt(Instant.now())
                .profileName(result.getProfileName())
                .commitSha(result.getCommitSha())
                .hasDegradations(hasDegradations)
                .degradations(degradations)
                .baselineStatus(baselineStatus)
                .build();
    }

    // Evaluation rules

    private void checkReqThreshold(String queryId,
                                    BenchmarkResult.QueryResult current,
                                    QueryEntry req,
                                    List<DegradationReport.Degradation> degradations) {
        if (req == null || !req.isHasReq()) return;
        long threshold = req.getMaxResponseTimeMs();
        if (current.getP95Ms() > threshold) {
            degradations.add(DegradationReport.Degradation.builder()
                    .queryId(queryId)
                    .type(DegradationReport.DegradationType.P95_EXCEEDED)
                    .observedValue(current.getP95Ms())
                    .thresholdValue(threshold)
                    .description(String.format(
                            "[%s] p95=%.0f ms exceeds maxResponseTimeMs=%d ms (@Req)",
                            queryId, current.getP95Ms(), threshold))
                    .build());
        }
    }

    private void checkPlanChange(String queryId,
                                  BenchmarkResult.QueryResult current,
                                  BenchmarkResult.QueryResult baselineResult,
                                  QueryEntry req,
                                  List<DegradationReport.Degradation> degradations) {
        if (req == null || req.isAllowPlanChange()) return;
        if (baselineResult == null || baselineResult.getPlanCost() == 0.0) return;

        double baseCost  = baselineResult.getPlanCost();
        double tolerance = baseCost * (1 + planCostTolerancePct);
        if (current.getPlanCost() <= tolerance) return;

        double slaRiskPct   = current.getSlaRiskPct();
        double thresholdPct = planChangeSlaRiskThresholdPct * 100;
        // Observed relative cost increase, reported in the message.
        int costIncreasePct = (int) Math.round((current.getPlanCost() / baseCost - 1.0) * 100);

        String baselineNode = adapter.parsePlanRootNode(baselineResult.getExecutionPlanText());
        String currentNode  = adapter.parsePlanRootNode(current.getExecutionPlanText());
        boolean nodeChanged = !baselineNode.isEmpty() && !currentNode.isEmpty()
                && !baselineNode.equalsIgnoreCase(currentNode);

        if (slaRiskPct >= thresholdPct) {
            // The query already uses a significant part of its SLA budget: block.
            String description = nodeChanged
                    ? String.format(
                            "[%s] plan changed: '%s' → '%s' (cost %.2f → %.2f, +%d%%); " +
                            "allowPlanChange=false (slaRisk=%.0f%% >= %.0f%% threshold)",
                            queryId, baselineNode, currentNode,
                            baseCost, current.getPlanCost(), costIncreasePct,
                            slaRiskPct, thresholdPct)
                    : String.format(
                            "[%s] plan cost=%.2f exceeds baseline=%.2f (+%d%%); " +
                            "allowPlanChange=false (slaRisk=%.0f%% >= %.0f%% threshold)",
                            queryId, current.getPlanCost(), baseCost,
                            costIncreasePct, slaRiskPct, thresholdPct);

            degradations.add(DegradationReport.Degradation.builder()
                    .queryId(queryId)
                    .type(DegradationReport.DegradationType.PLAN_CHANGED)
                    .observedValue(current.getPlanCost())
                    .thresholdValue(tolerance)
                    .description(description)
                    .build());
        } else {
            // Well within the SLA budget: warn only; the nightly run at higher volume
            // confirms whether the new plan degrades at scale.
            String description = nodeChanged
                    ? String.format(
                            "[%s] plan changed: '%s' → '%s' (cost %.2f → %.2f, +%d%%) — " +
                            "warning only (p95=%.0f ms is %.0f%% of SLA; nightly will confirm at scale)",
                            queryId, baselineNode, currentNode,
                            baseCost, current.getPlanCost(), costIncreasePct,
                            current.getP95Ms(), slaRiskPct)
                    : String.format(
                            "[%s] plan cost=%.2f increased +%d%% vs baseline — " +
                            "warning only (p95=%.0f ms is %.0f%% of SLA; nightly will confirm at scale)",
                            queryId, current.getPlanCost(), costIncreasePct,
                            current.getP95Ms(), slaRiskPct);

            degradations.add(DegradationReport.Degradation.builder()
                    .queryId(queryId)
                    .type(DegradationReport.DegradationType.BASELINE_EXCEEDED)
                    .observedValue(current.getPlanCost())
                    .thresholdValue(baseCost)
                    .description(description)
                    .build());
        }
    }

    private void checkSloProximity(String queryId,
                                    BenchmarkResult.QueryResult current,
                                    QueryEntry req,
                                    List<DegradationReport.Degradation> degradations) {
        if (req == null || !req.isHasReq()) return;
        double slo = req.getMaxResponseTimeMs() * sloProximityPct;
        if (current.getP95Ms() > slo && current.getP95Ms() <= req.getMaxResponseTimeMs()) {
            degradations.add(DegradationReport.Degradation.builder()
                    .queryId(queryId)
                    .type(DegradationReport.DegradationType.SLO_PROXIMITY)
                    .observedValue(current.getP95Ms())
                    .thresholdValue(slo)
                    .description(String.format(
                            "[%s] p95=%.0f ms exceeds internal SLO of %.0f ms (%.0f%% of SLA=%d ms) — risk zone",
                            queryId, current.getP95Ms(), slo,
                            sloProximityPct * 100, req.getMaxResponseTimeMs()))
                    .build());
        }
    }

    private void checkBaselineExceeded(String queryId,
                                        BenchmarkResult.QueryResult current,
                                        BenchmarkResult.QueryResult baselineResult,
                                        List<DegradationReport.Degradation> degradations) {
        if (baselineResult == null || baselineResult.getP95Ms() == 0.0) return;

        double baseP95   = baselineResult.getP95Ms();
        double threshold = baseP95 * (1 + baselineTolerancePct);
        if (current.getP95Ms() > threshold
                && current.getP95Ms() - baseP95 >= baselineMinDeltaMs) {
            degradations.add(DegradationReport.Degradation.builder()
                    .queryId(queryId)
                    .type(DegradationReport.DegradationType.BASELINE_EXCEEDED)
                    .observedValue(current.getP95Ms())
                    .thresholdValue(baseP95)
                    .description(String.format(
                            "[%s] p95=%.0f ms — %.1fx slower than last approved measurement (%.0f ms)",
                            queryId, current.getP95Ms(),
                            current.getP95Ms() / baseP95, baseP95))
                    .build());
        }
    }
}
