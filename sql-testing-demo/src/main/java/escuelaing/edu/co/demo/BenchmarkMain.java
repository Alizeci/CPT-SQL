package escuelaing.edu.co.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import escuelaing.edu.co.domain.model.BenchmarkResult;
import escuelaing.edu.co.domain.model.DegradationReport;
import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.infrastructure.analysis.BaselineManager;
import escuelaing.edu.co.infrastructure.analysis.DegradationDetector;
import escuelaing.edu.co.infrastructure.benchmark.BenchmarkRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;


/**
 * Punto de entrada del motor de benchmark CPT-SQL (Fases 3 y 4).
 *
 * <p>Lee el {@code load-profile.json} generado por {@link EcommerceSimulator}
 * (Fase 2), ejecuta el benchmark sobre la BD espejo y detecta regresiones
 * contra el {@code baseline.json} commiteado.</p>
 *
 * <h3>Uso local</h3>
 * <pre>
 * ./gradlew :sql-testing-demo:runBenchmark
 * </pre>
 *
 * <h3>Uso en CI (GitHub Actions)</h3>
 * <pre>
 * java -jar sql-testing-demo.jar \
 *   --loadtest.benchmark.testProfile=light \
 *   --loadtest.mirror.schema.script=sql-testing-demo/src/main/resources/schema-ecommerce.sql
 * </pre>
 *
 * <h3>Códigos de salida</h3>
 * <ul>
 *   <li>{@code 0} — sin regresiones críticas (puede haber advertencias BASELINE_EXCEEDED).</li>
 *   <li>{@code 1} — SLA violado (P95_EXCEEDED) o plan de ejecución cambiado (PLAN_CHANGED).</li>
 * </ul>
 */
@SpringBootApplication(scanBasePackages = "escuelaing.edu.co.infrastructure")
public class BenchmarkMain {

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(BenchmarkMain.class, args)));
    }

    @Bean
    CommandLineRunner run(BenchmarkRunner benchmarkRunner,
                          DegradationDetector detector,
                          BaselineManager baselineManager) {
        return args -> {
            // --- Leer load-profile.json ---
            ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
            File profileFile = new File("load-profile.json");
            if (!profileFile.exists()) {
                System.err.println("[BenchmarkMain] load-profile.json no encontrado.");
                System.err.println("  Ejecuta primero: ./gradlew :sql-testing-demo:runSimulator");
                System.exit(1);
            }
            LoadProfile profile = mapper.readValue(profileFile, LoadProfile.class);
            System.out.println("[BenchmarkMain] Perfil cargado: "
                    + profile.getTotalSamples() + " muestras, "
                    + profile.getQueries().size() + " queries.");

            // --- Fase 3 + Fase 4: benchmark y persistencia ---
            String commitSha = System.getenv("GITHUB_SHA");
            BenchmarkResult result = benchmarkRunner.run(profile, commitSha);

            // --- Detección de regresiones vs baseline.json ---
            DegradationReport report = detector.detect(result);

            // --- Reporte de resultados ---
            System.out.println("\n=== BENCHMARK RESULT ===");
            System.out.println("Veredicto : " + result.getOverallVerdict());
            System.out.println("Perfil    : " + result.getTestProfileName());
            System.out.println("Muestras  : " + result.getTotalOperations());
            result.getQueries().forEach((qid, qr) -> {
                String risk = "";
                if (qr.getSlaRiskPct() >= 90) risk = "  !! CRITICO: " + String.format("%.0f%%", qr.getSlaRiskPct()) + " del SLA";
                else if (qr.getSlaRiskPct() >= 70) risk = "  !! RIESGO: " + String.format("%.0f%%", qr.getSlaRiskPct()) + " del SLA";
                System.out.printf("  %-35s p95=%5.0f ms  sla=%.1f%%  verdict=%s%s%n",
                        qid, qr.getP95Ms(), qr.getSlaComplianceRate(), qr.getVerdict(), risk);
            });

            // --- Clasificar regresiones: críticas (exit 1) vs advertencias (exit 0) ---
            List<DegradationReport.Regression> blocking = report.getRegressions().stream()
                    .filter(r -> r.getType() == DegradationReport.RegressionType.P95_EXCEEDED
                              || r.getType() == DegradationReport.RegressionType.PLAN_CHANGED
                              || r.getType() == DegradationReport.RegressionType.SLO_PROXIMITY)
                    .collect(java.util.stream.Collectors.toList());

            List<DegradationReport.Regression> warnings = report.getRegressions().stream()
                    .filter(r -> r.getType() == DegradationReport.RegressionType.BASELINE_EXCEEDED)
                    .collect(java.util.stream.Collectors.toList());

            if (!warnings.isEmpty()) {
                System.out.println("\n=== ADVERTENCIAS DE DEGRADACIÓN (no bloquean) ===");
                warnings.forEach(r -> System.out.println("  " + r.getDescription()));

                // Solo publicar el comentario de advertencia si no hay regresiones críticas.
                // Si hay P95_EXCEEDED, el pipeline ya falla — el comentario sería contradictorio.
                if (blocking.isEmpty()) {
                    try (PrintWriter pw = new PrintWriter("benchmark-warnings.txt")) {
                        pw.println("🟡 **CPT-SQL — Advertencia de degradación**\n");
                        warnings.forEach(r -> {
                            BenchmarkResult.QueryResult qr = result.getQueries().get(r.getQueryId());
                            String slaLine = (qr != null && qr.getSlaRiskPct() < 100)
                                    ? String.format("SLA aún cumplido (%.0f%% del límite) — el merge está permitido.", qr.getSlaRiskPct())
                                    : "SLA aún cumplido — el merge está permitido.";
                            pw.println(r.getDescription());
                            pw.println(slaLine);
                            pw.println();
                        });
                    } catch (Exception e) {
                        System.err.println("[BenchmarkMain] No se pudo escribir benchmark-warnings.txt: " + e.getMessage());
                    }
                }
            }

            if (!blocking.isEmpty()) {
                System.err.println("\n=== REGRESIONES CRÍTICAS DETECTADAS ===");
                blocking.forEach(r ->
                    System.err.println("  [FALLO] [" + r.getType() + "] " + r.getDescription()));
                System.exit(1);
            }

            // --- Actualizar baseline según contexto de ejecución ---
            // GITHUB_EVENT_NAME="pull_request" → solo escribe baseline-candidate.json (el workflow
            // lo cachea y lo commitea tras el merge, sin volver a correr el benchmark).
            // GITHUB_EVENT_NAME="push" o ejecución local → actualiza baseline.json directamente.
            boolean isPullRequest = "pull_request".equals(System.getenv("GITHUB_EVENT_NAME"));
            if (result.getOverallVerdict() == BenchmarkResult.Verdict.PASS) {
                if (isPullRequest) {
                    baselineManager.saveAs(result, "baseline-candidate.json");
                    System.out.println("\n[BenchmarkMain] PR check — baseline-candidate.json escrito para post-merge.");
                } else {
                    baselineManager.save(result);
                    System.out.println("\n[BenchmarkMain] baseline.json actualizado (post-merge).");
                }
            }

            System.out.println("[BenchmarkMain] Pipeline OK." + (warnings.isEmpty() ? "" : " (con advertencias de degradación)"));
        };
    }
}
