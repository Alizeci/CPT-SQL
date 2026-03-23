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
 *   <li>{@code 0} — benchmark PASS, baseline actualizado.</li>
 *   <li>{@code 1} — regresiones detectadas o benchmark FAIL.</li>
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

            if (report.isHasRegressions()) {
                System.err.println("\n=== REGRESIONES DETECTADAS ===");
                report.getRegressions().forEach(r ->
                    System.err.println("  [" + r.getType() + "] " + r.getDescription()));
                System.exit(1);
            }

            // --- Actualizar baseline solo si PASS ---
            if (result.getOverallVerdict() == BenchmarkResult.Verdict.PASS) {
                baselineManager.save(result);
                System.out.println("\n[BenchmarkMain] baseline.json actualizado.");
            }

            System.out.println("[BenchmarkMain] Sin regresiones. Pipeline OK.");
        };
    }
}
