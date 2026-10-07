package escuelaing.edu.co.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import escuelaing.edu.co.domain.model.BenchmarkResult;
import escuelaing.edu.co.domain.model.DegradationReport;
import escuelaing.edu.co.domain.model.LoadProfile;
import escuelaing.edu.co.domain.model.validation.DataQuality;
import escuelaing.edu.co.domain.model.validation.MirrorAccuracy;
import escuelaing.edu.co.domain.model.validation.ValidationReport;
import escuelaing.edu.co.infrastructure.analysis.BaselineManager;
import escuelaing.edu.co.infrastructure.analysis.DegradationDetector;
import escuelaing.edu.co.infrastructure.benchmark.BenchmarkRunner;
import escuelaing.edu.co.infrastructure.benchmark.FidelityValidator;
import escuelaing.edu.co.infrastructure.capture.LoadProfileMerger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Entry point of the CPT-SQL gate: runs the fidelity validation, replays the load profile
 * against the mirror database and evaluates the result against the {@code @Req} contracts
 * and the committed {@code baseline.json}.
 *
 * <p>Pipeline: load {@code load-profile.json} (protected statistics from the capture
 * environment); optionally merge the PR's SQL from {@code loadtest.profile.sqlPath};
 * run the fidelity validation (reported, never blocking); run the benchmark; detect
 * degradations; print the report.</p>
 *
 * <p>Scans {@code escuelaing.edu.co.*} so that adopting applications can
 * contribute beans (e.g. a custom {@link escuelaing.edu.co.infrastructure.capture.SanitizationStrategy})
 * simply by placing them on the classpath under that namespace.</p>
 *
 * <h3>Exit codes</h3>
 * <ul>
 *   <li>{@code 0} — no blocking degradations (warnings may be present).</li>
 *   <li>{@code 1} — a blocking degradation (P95_EXCEEDED, SLO_PROXIMITY, PLAN_CHANGED),
 *       a missing load profile, or an exception during fidelity validation. A fidelity
 *       FAIL status does not change the exit code.</li>
 * </ul>
 */
@SpringBootApplication(scanBasePackages = {"escuelaing.edu.co"})
public class BenchmarkMain {

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(BenchmarkMain.class, args)));
    }

    @Bean
    CommandLineRunner run(BenchmarkRunner benchmarkRunner,
                          FidelityValidator fidelityValidator,
                          DegradationDetector detector,
                          BaselineManager baselineManager,
                          @Value("${loadtest.profile.path:load-profile.json}") String profilePath,
                          @Value("${loadtest.profile.sqlPath:}") String sqlPath,
                          @Value("${loadtest.benchmark.testProfile:normal}") String testProfile) {
        return args -> {
            ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

            // Load the production load profile (protected statistics).
            File profileFile = new File(profilePath);
            if (!profileFile.exists()) {
                System.err.println("[BenchmarkMain] Profile not found: " + profilePath);
                System.err.println("  Run first: ./gradlew runSimulator");
                System.exit(1);
            }
            LoadProfile profile = mapper.readValue(profileFile, LoadProfile.class);
            System.out.println("[BenchmarkMain] Profile loaded: "
                    + profile.getTotalSamples() + " samples, "
                    + profile.getQueries().size() + " queries.");

            // When a separate PR profile is given, replace only capturedSql per queryId;
            // the production statistics (p95Ms, meanMs, callsPerMinute, ...) are kept.
            boolean hasSqlProfile = sqlPath != null && !sqlPath.isBlank()
                    && !sqlPath.equals(profilePath);
            if (hasSqlProfile) {
                File sqlFile = new File(sqlPath);
                if (sqlFile.exists()) {
                    LoadProfile prProfile = mapper.readValue(sqlFile, LoadProfile.class);
                    int merged = LoadProfileMerger.merge(profile, prProfile);
                    System.out.println("[BenchmarkMain] PR SQL merged for " + merged + " queries.");
                } else {
                    System.err.println("[BenchmarkMain] PR SQL profile not found: " + sqlPath
                            + " — using capturedSql from production profile.");
                }
            }

            // Fidelity validation is reported for diagnosis but never blocks: runs stay
            // comparable even when the mirror differs from production, because the detector
            // only needs mirror-to-mirror consistency. Only an exception aborts (exit 1).
            // Mirror accuracy is evaluated for every test profile except "normal".
            boolean applicable = !"normal".equals(testProfile);
            try {
                ValidationReport validationReport = fidelityValidator.performValidation(profile, applicable);
                benchmarkRunner.persistValidationReport(validationReport);

                DataQuality    dq = validationReport.getDataQuality();
                MirrorAccuracy ma = validationReport.getMirrorAccuracy();

                System.out.printf("[BenchmarkMain] Data quality: %s, privacy=%s (ε=%.1f, δ=%.0e), FK shapes=%d%n",
                        dq.getStatus(), dq.getPrivacyMode(), dq.getEpsilonTotal(), dq.getDelta(),
                        dq.getDpForeignKeys() != null ? dq.getDpForeignKeys().size() : 0);

                if (!ma.isApplicable()) {
                    System.out.println("[BenchmarkMain] Mirror accuracy: N/A"
                            + " (nightly by design — captures volume drift)");
                } else {
                    System.out.printf("[BenchmarkMain] Mirror accuracy: %s"
                            + " (mirror volume: %d rows/table)%n",
                            ma.getStatus(), ma.getMirrorRowsPerTable());
                    if ("FAIL".equals(ma.getStatus())) {
                        System.err.println("[BenchmarkMain] Mirror accuracy returned FAIL."
                                + " Continuing benchmark; interpret absolute latencies with caution."
                                + " See VALIDATION_REPORT.json for details.");
                    }
                }
            } catch (Exception e) {
                Throwable root = e;
                while (root.getCause() != null && root.getCause() != root) root = root.getCause();
                System.err.println("[BenchmarkMain] Fidelity validation error: " + e.getMessage()
                        + (root != e ? " — root cause: " + root : "")
                        + " — aborting benchmark.");
                System.exit(1);
            }

            // Benchmark the mirror database; the result is also written to build/results.
            String commitSha = System.getenv("GITHUB_SHA");
            BenchmarkResult result = benchmarkRunner.run(profile, commitSha);

            DegradationReport report = detector.detect(result);
            System.out.println("[BenchmarkMain] Baseline: " + report.getBaselineStatus());

            // Blocking degradations exit with 1; BASELINE_EXCEEDED is a warning only.
            List<DegradationReport.Degradation> blocking = report.getDegradations().stream()
                    .filter(r -> r.getType() == DegradationReport.DegradationType.P95_EXCEEDED
                              || r.getType() == DegradationReport.DegradationType.PLAN_CHANGED
                              || r.getType() == DegradationReport.DegradationType.SLO_PROXIMITY)
                    .collect(Collectors.toList());

            List<DegradationReport.Degradation> warnings = report.getDegradations().stream()
                    .filter(r -> r.getType() == DegradationReport.DegradationType.BASELINE_EXCEEDED)
                    .collect(Collectors.toList());

            // FAIL on any blocking degradation (even with p95 within the SLA, e.g. PLAN_CHANGED)
            // or when a query verdict is FAIL. The exit code depends only on blocking
            // degradations; a FAIL verdict alone only prevents the baseline update below.
            BenchmarkResult.Verdict gateVerdict = (!blocking.isEmpty()
                    || result.getOverallVerdict() == BenchmarkResult.Verdict.FAIL)
                    ? BenchmarkResult.Verdict.FAIL
                    : BenchmarkResult.Verdict.PASS;

            System.out.println("\n=== BENCHMARK RESULT ===");
            System.out.println("Verdict  : " + gateVerdict);
            System.out.println("Profile  : " + result.getTestProfileName());
            System.out.println("Samples  : " + result.getTotalOperations());
            result.getQueries().forEach((qid, qr) -> {
                String risk = "";
                if (qr.getSlaRiskPct() >= 90)     risk = "  !! CRITICAL: " + String.format("%.0f%%", qr.getSlaRiskPct()) + " of SLA";
                else if (qr.getSlaRiskPct() >= 70) risk = "  !! WARNING: "  + String.format("%.0f%%", qr.getSlaRiskPct()) + " of SLA";
                System.out.printf("  %-35s p95=%5.0f ms  sla=%.1f%%  verdict=%s%s%n",
                        qid, qr.getP95Ms(), qr.getSlaComplianceRate(), qr.getVerdict(), risk);
            });

            if (!warnings.isEmpty()) {
                System.out.println("\n=== DEGRADATION WARNINGS (non-blocking) ===");
                warnings.forEach(r -> System.out.println("  " + r.getDescription()));

                // Write benchmark-warnings.txt (for a PR comment) only when nothing blocks
                // and at least one warned query already uses >= 50 % of its SLA.

                boolean shouldPublish = blocking.isEmpty() && warnings.stream().anyMatch(r -> {
                    BenchmarkResult.QueryResult qr = result.getQueries().get(r.getQueryId());
                    return qr != null && qr.getSlaRiskPct() >= 50.0;
                });
                if (shouldPublish) {
                    try (PrintWriter pw = new PrintWriter("benchmark-warnings.txt")) {
                        pw.println("🟡 **CPT-SQL — Degradation warning**\n");
                        warnings.forEach(r -> {
                            BenchmarkResult.QueryResult qr = result.getQueries().get(r.getQueryId());
                            String slaLine = (qr != null && qr.getSlaRiskPct() < 100)
                                    ? String.format("SLA still met (%.0f%% of limit) — merge is allowed.", qr.getSlaRiskPct())
                                    : "SLA still met — merge is allowed.";
                            pw.println(r.getDescription());
                            pw.println(slaLine);
                            pw.println();
                        });
                    } catch (Exception e) {
                        System.err.println("[BenchmarkMain] Could not write benchmark-warnings.txt: " + e.getMessage());
                    }
                }
            }

            if (!blocking.isEmpty()) {
                System.err.println("\n=== CRITICAL DEGRADATIONS DETECTED ===");
                blocking.forEach(r ->
                    System.err.println("  [FAIL] [" + r.getType() + "] " + r.getDescription()));
                System.exit(1);
            }

            // Overwrites the local baseline.json on PASS. It becomes the reference only once
            // committed by the post-merge update-baseline workflow, so the baseline always
            // reflects approved code.
            if (gateVerdict == BenchmarkResult.Verdict.PASS) {
                baselineManager.save(result);
                System.out.println("\n[BenchmarkMain] baseline.json updated.");
            }

            System.out.println("[BenchmarkMain] Pipeline OK."
                    + (warnings.isEmpty() ? "" : " (with degradation warnings)"));
        };
    }
}
