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
 * Detecta regresiones de rendimiento comparando el {@link BenchmarkResult}
 * contra dos fuentes de referencia:
 *
 * <ol>
 *   <li><b>Umbrales {@code @Req} (Fase 1):</b> {@code maxResponseTimeMs} y
 *       {@code allowPlanChange} declarados en el código fuente y leídos desde
 *       {@code queries.json} vía {@link QueryRegistryLoader}.</li>
 *   <li><b>Línea base histórica:</b> p95 guardado en {@code baseline.json} por
 *       el {@link BaselineManager}. Una regresión se marca cuando el p95
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
     * @return informe con la lista de regresiones (puede estar vacía)
     */
    public DegradationReport detect(BenchmarkResult result) {
        Map<String, QueryEntry> registry = queryRegistry.getRegistry();
        Map<String, BenchmarkResult.QueryResult> baseline = baselineManager.load();

        List<DegradationReport.Regression> regressions = new ArrayList<>();

        for (Map.Entry<String, BenchmarkResult.QueryResult> entry
                : result.getQueries().entrySet()) {

            String queryId  = entry.getKey();
            BenchmarkResult.QueryResult current = entry.getValue();
            QueryEntry req  = registry.get(queryId);

            checkReqThreshold(queryId, current, req, regressions);
            checkPlanChange(queryId, current, baseline.get(queryId), req, regressions);
            checkBaselineExceeded(queryId, current, baseline.get(queryId), regressions);
        }

        boolean hasRegressions = !regressions.isEmpty();
        LOG.info("[DegradationDetector] " + (hasRegressions
                ? regressions.size() + " regresión(es) detectada(s)."
                : "Sin regresiones."));

        return DegradationReport.builder()
                .generatedAt(Instant.now())
                .profileName(result.getProfileName())
                .commitSha(result.getCommitSha())
                .hasRegressions(hasRegressions)
                .regressions(regressions)
                .build();
    }

    // -------------------------------------------------------------------------
    // Reglas de evaluación
    // -------------------------------------------------------------------------

    private void checkReqThreshold(String queryId,
                                    BenchmarkResult.QueryResult current,
                                    QueryEntry req,
                                    List<DegradationReport.Regression> out) {
        if (req == null || !req.isHasReq()) return;
        long threshold = req.getMaxResponseTimeMs();
        if (current.getP95Ms() > threshold) {
            out.add(DegradationReport.Regression.builder()
                    .queryId(queryId)
                    .type(DegradationReport.RegressionType.P95_EXCEEDED)
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
                                  List<DegradationReport.Regression> out) {
        if (req == null || req.isAllowPlanChange()) return;
        if (baselineResult == null || baselineResult.getPlanCost() == 0.0) return;

        double baseCost   = baselineResult.getPlanCost();
        double tolerance  = baseCost * (1 + planCostTolerancePct);
        if (current.getPlanCost() > tolerance) {
            out.add(DegradationReport.Regression.builder()
                    .queryId(queryId)
                    .type(DegradationReport.RegressionType.PLAN_CHANGED)
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

    private void checkBaselineExceeded(String queryId,
                                        BenchmarkResult.QueryResult current,
                                        BenchmarkResult.QueryResult baselineResult,
                                        List<DegradationReport.Regression> out) {
        if (baselineResult == null || baselineResult.getP95Ms() == 0.0) return;

        double baseP95   = baselineResult.getP95Ms();
        double threshold = baseP95 * (1 + baselineTolerancePct);
        if (current.getP95Ms() > threshold) {
            out.add(DegradationReport.Regression.builder()
                    .queryId(queryId)
                    .type(DegradationReport.RegressionType.BASELINE_EXCEEDED)
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
