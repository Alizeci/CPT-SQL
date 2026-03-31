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
 * Detecta degradaciones de rendimiento comparando el {@link BenchmarkResult}
 * contra dos fuentes de referencia:
 *
 * <ol>
 *   <li><b>Umbrales {@code @Req} (Fase 1):</b> {@code maxResponseTimeMs} y
 *       {@code allowPlanChange} declarados en el código fuente y leídos desde
 *       {@code queries.json} vía {@link QueryRegistryLoader}.</li>
 *   <li><b>Línea base histórica:</b> p95 guardado en {@code baseline.json} por
 *       el {@link BaselineManager}. Una degradación se marca cuando el p95
 *       actual supera la línea base en más de {@code baselineTolerancePct} %
 *       (10 % por defecto).</li>
 * </ol>
 *
 * <h3>Orden de evaluación</h3>
 * <ol>
 *   <li>{@code P95_EXCEEDED} — p95 medido &gt; {@code maxResponseTimeMs}.</li>
 *   <li>{@code PLAN_CHANGED} — el costo del plan cambió y {@code allowPlanChange = false}.</li>
 *   <li>{@code BASELINE_EXCEEDED} — p95 medido &gt; p95 base × (1 + tolerancia).</li>
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

        boolean hasDegradations = !degradations.isEmpty();
        LOG.info("[DegradationDetector] " + (hasDegradations
                ? degradations.size() + " degradación(es) detectada(s)."
                : "Sin degradaciones."));

        return DegradationReport.builder()
                .generatedAt(Instant.now())
                .profileName(result.getProfileName())
                .commitSha(result.getCommitSha())
                .hasDegradations(hasDegradations)
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
                            "[%s] p95=%.0f ms supera maxResponseTimeMs=%d ms de @Req",
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

        double baseCost   = baselineResult.getPlanCost();
        double tolerance  = baseCost * (1 + planCostTolerancePct);
        if (current.getPlanCost() > tolerance) {
            degradations.add(DegradationReport.Degradation.builder()
                    .queryId(queryId)
                    .type(DegradationReport.DegradationType.PLAN_CHANGED)
                    .observedValue(current.getPlanCost())
                    .thresholdValue(tolerance)
                    .description(String.format(
                            "[%s] Costo del plan=%.2f supera la línea base=%.2f (+%d%%); " +
                            "allowPlanChange=false",
                            queryId, current.getPlanCost(), baseCost,
                            (int)(planCostTolerancePct * 100)))
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
                            "[%s] p95=%.0f ms supera el SLO interno de %.0f ms (%.0f%% del SLA=%d ms) — zona de riesgo",
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
                            "[%s] p95=%.0f ms — %.1fx más lento que la última medición aprobada (%.0f ms)",
                            queryId, current.getP95Ms(),
                            current.getP95Ms() / baseP95, baseP95))
                    .build());
        }
    }
}
