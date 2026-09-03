package escuelaing.edu.co.infrastructure.analysis;

import escuelaing.edu.co.domain.model.BenchmarkResult;
import escuelaing.edu.co.domain.model.DegradationReport;
import escuelaing.edu.co.domain.model.QueryEntry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Detects performance regressions by comparing a {@link BenchmarkResult} against
 * two reference sources:
 *
 * <ol>
 *   <li><b>{@code @Req} thresholds (Phase 1):</b> {@code maxResponseTimeMs} and
 *       {@code allowPlanChange} declared in source code and read from
 *       {@code queries.json} via {@link QueryRegistryLoader}.</li>
 *   <li><b>Historical baseline:</b> p95 stored in {@code baseline.json} by
 *       {@link BaselineManager}. A regression is flagged when the current p95
 *       exceeds the baseline by more than {@code baselineTolerancePct}
 *       (10 % by default).</li>
 * </ol>
 *
 * <h3>Evaluation order</h3>
 * <ol>
 *   <li>{@code P95_EXCEEDED} — measured p95 &gt; {@code maxResponseTimeMs}.</li>
 *   <li>{@code SLO_PROXIMITY} — measured p95 &gt; {@code sloProximityPct} × SLA.</li>
 *   <li>{@code PLAN_CHANGED} — plan cost increased beyond tolerance
 *       <em>and</em> {@code slaRiskPct} &ge; {@code planChangeSlaRiskThresholdPct}
 *       (50 % by default). Below that threshold the plan change is reported as
 *       a {@code BASELINE_EXCEEDED} warning: the query is still fast, but the
 *       nightly benchmark should confirm it holds at production volume.</li>
 *   <li>{@code BASELINE_EXCEEDED} — measured p95 &gt; baseline p95 × (1 + tolerance).</li>
 * </ol>
 */
@Component
public class DegradationDetector {

    private static final Logger LOG = Logger.getLogger(DegradationDetector.class.getName());

    @Value("${loadtest.detector.baselineTolerancePct:0.10}")
    private double baselineTolerancePct;

    @Value("${loadtest.detector.planCostTolerancePct:0.20}")
    private double planCostTolerancePct;

    @Value("${loadtest.detector.sloProximityPct:0.80}")
    private double sloProximityPct;

    /**
     * Minimum SLA risk percentage at which a plan cost increase becomes a blocking
     * {@code PLAN_CHANGED}. Below this threshold the plan change is downgraded to a
     * {@code BASELINE_EXCEEDED} warning: the query is still well within its SLA budget,
     * so the nightly benchmark (higher volume) is the right place to confirm the impact.
     * Default: 50 % — i.e., p95 must already consume at least half the SLA budget.
     */
    @Value("${loadtest.detector.planChangeSlaRiskThresholdPct:0.50}")
    private double planChangeSlaRiskThresholdPct;

    private final QueryRegistryLoader queryRegistry;
    private final BaselineManager baselineManager;

    public DegradationDetector(QueryRegistryLoader queryRegistry,
                                BaselineManager baselineManager) {
        this.queryRegistry   = queryRegistry;
        this.baselineManager = baselineManager;
    }

    // -------------------------------------------------------------------------
    // API pública
    // -------------------------------------------------------------------------

    /**
     * Evalúa el {@code result} y genera un {@link DegradationReport}.
     *
     * @param result resultado del benchmark a evaluar
     * @return informe con la lista de degradaciones (puede estar vacía)
     */
    public DegradationReport detect(BenchmarkResult result) {
        Map<String, QueryEntry> registry = queryRegistry.getRegistry();
        Map<String, BenchmarkResult.QueryResult> baseline = baselineManager.load();

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

        // Eliminar BASELINE_EXCEEDED de queries que ya tienen una degradación bloqueante.
        // Si SLO_PROXIMITY o P95_EXCEEDED ya detectaron el problema, el warning es redundante.
        java.util.Set<String> blockedQueries = degradations.stream()
                .filter(r -> r.getType() != DegradationReport.DegradationType.BASELINE_EXCEEDED)
                .map(DegradationReport.Degradation::getQueryId)
                .collect(java.util.stream.Collectors.toSet());

        degradations.removeIf(r -> r.getType() == DegradationReport.DegradationType.BASELINE_EXCEEDED
                && blockedQueries.contains(r.getQueryId()));

        boolean hasRegressions = !degradations.isEmpty();
        LOG.info("[DegradationDetector] " + (hasRegressions
                ? degradations.size() + " degradation(s) detected."
                : "No degradations detected."));

        return DegradationReport.builder()
                .generatedAt(Instant.now())
                .profileName(result.getProfileName())
                .commitSha(result.getCommitSha())
                .hasDegradations(hasRegressions)
                .degradations(degradations)
                .build();
    }

    // -------------------------------------------------------------------------
    // Reglas de evaluación
    // -------------------------------------------------------------------------

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

        double slaRiskPct = current.getSlaRiskPct();   // 0–100 scale
        double thresholdPct = planChangeSlaRiskThresholdPct * 100;

        if (slaRiskPct >= thresholdPct) {
            // Plan changed AND the query already consumes a significant portion of its
            // SLA budget — block the merge.
            degradations.add(DegradationReport.Degradation.builder()
                    .queryId(queryId)
                    .type(DegradationReport.DegradationType.PLAN_CHANGED)
                    .observedValue(current.getPlanCost())
                    .thresholdValue(tolerance)
                    .description(String.format(
                            "[%s] plan cost=%.2f exceeds baseline=%.2f (+%d%%); " +
                            "allowPlanChange=false (slaRisk=%.0f%% >= %.0f%% threshold)",
                            queryId, current.getPlanCost(), baseCost,
                            (int)(planCostTolerancePct * 100), slaRiskPct, thresholdPct))
                    .build());
        } else {
            // Plan changed but the query is well within its SLA budget — warn only.
            // The nightly benchmark at production volume will confirm whether the new
            // plan actually degrades under real data scale.
            degradations.add(DegradationReport.Degradation.builder()
                    .queryId(queryId)
                    .type(DegradationReport.DegradationType.BASELINE_EXCEEDED)
                    .observedValue(current.getPlanCost())
                    .thresholdValue(baseCost)
                    .description(String.format(
                            "[%s] plan cost=%.2f increased +%d%% vs baseline — warning only " +
                            "(p95=%.0f ms is %.0f%% of SLA; nightly will confirm at scale)",
                            queryId, current.getPlanCost(),
                            (int)(planCostTolerancePct * 100),
                            current.getP95Ms(), slaRiskPct))
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
        if (current.getP95Ms() > threshold) {
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
