package escuelaing.edu.co.infrastructure.capture;

import com.fasterxml.jackson.databind.ObjectMapper;
import escuelaing.edu.co.domain.model.LoadProfile;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Orchestrates production capture windows: enables {@link CaptureToggle},
 * waits for the configured duration, then builds and persists
 * {@code load-profile.json} via {@link LoadProfileBuilder}.
 *
 * <p>Triggered automatically by a nightly cron or on demand via
 * {@code POST /loadtest/phase2/run}. Concurrent executions are prevented
 * with an {@link AtomicBoolean} guard.</p>
 *
 * <h3>Configuration (application.properties)</h3>
 * <pre>
 * loadtest.phase2.cron=0 0 2 * * *
 * loadtest.phase2.captureWindowSecs=120
 * loadtest.phase2.outputPath=load-profile.json
 * </pre>
 */
@Component
public class CaptureWindowOrchestrator {

    private static final Logger LOG = Logger.getLogger(CaptureWindowOrchestrator.class.getName());

    private final CaptureToggle     captureToggle;
    private final LoadProfileBuilder profileBuilder;
    private final ObjectMapper      mapper;

    private final AtomicBoolean running = new AtomicBoolean(false);

    @Value("${loadtest.phase2.captureWindowSecs:120}")
    private int captureWindowSecs;

    @Value("${loadtest.phase2.outputPath:load-profile.json}")
    private String outputPath;

    public CaptureWindowOrchestrator(CaptureToggle captureToggle,
                                     LoadProfileBuilder profileBuilder,
                                     ObjectMapper mapper) {
        this.captureToggle  = captureToggle;
        this.profileBuilder = profileBuilder;
        this.mapper         = mapper;
    }

    // Scheduled trigger — nightly cron (configurable)
    @Scheduled(cron = "${loadtest.phase2.cron:0 0 2 * * *}")
    public void runScheduled() {
        LOG.info("[CaptureWindowOrchestrator] Scheduled trigger (cron 2 AM UTC).");
        executeCaptureWindow(captureWindowSecs);
    }

    // Manual trigger — POST /loadtest/phase2/run
    @RestController
    @RequestMapping("/loadtest/phase2")
    public class Controller {

        @PostMapping("/run")
        public Map<String, Object> runManual(
                @RequestParam(defaultValue = "0") int windowSecs) {
            int window = windowSecs > 0 ? windowSecs : captureWindowSecs;
            if (running.get()) {
                return Map.of("status", "SKIPPED", "reason", "A capture window is already running.");
            }
            int finalWindow = window;
            Thread t = new Thread(() -> executeCaptureWindow(finalWindow), "phase2-manual");
            t.setDaemon(true);
            t.start();
            return Map.of(
                    "status", "STARTED",
                    "captureWindowSecs", window,
                    "outputPath", outputPath);
        }
    }

    // Capture logic

    /**
     * Opens a capture window for {@code windowSecs} seconds, then builds and
     * persists the load profile. Restores the previous toggle state on exit.
     */
    void executeCaptureWindow(int windowSecs) {
        if (!running.compareAndSet(false, true)) {
            LOG.warning("[CaptureWindowOrchestrator] Skipped — a capture window is already running.");
            return;
        }

        boolean previousState = captureToggle.isEnabled();
        try {
            LOG.info(String.format("[CaptureWindowOrchestrator] Opening capture window (%d s)...", windowSecs));
            captureToggle.enable();
            Thread.sleep(windowSecs * 1_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warning("[CaptureWindowOrchestrator] Capture window interrupted.");
        } finally {
            captureToggle.setEnabled(previousState);
            running.set(false);
        }

        buildAndPersist();
    }

    private void buildAndPersist() {
        LoadProfile profile = profileBuilder.build();

        if (profile.getQueries() == null || profile.getQueries().isEmpty()) {
            LOG.warning("[CaptureWindowOrchestrator] Empty profile — no queries captured. "
                    + "Verify that JdbcWrapper is active and traffic is flowing.");
            return;
        }

        long nonZeroLatencies = profile.getQueries().values().stream()
                .filter(s -> s.getP95Ms() > 0)
                .count();
        if (nonZeroLatencies == 0) {
            LOG.warning("[CaptureWindowOrchestrator] All latencies are 0 ms — possible bug in JdbcWrapper.");
        }

        try {
            Path output = Path.of(outputPath);
            mapper.writeValue(output.toFile(), profile);
            LOG.info(String.format(
                    "[CaptureWindowOrchestrator] load-profile.json written: %d queries, path=%s",
                    profile.getQueries().size(), output.toAbsolutePath()));
        } catch (IOException e) {
            LOG.severe("[CaptureWindowOrchestrator] Failed to persist load-profile.json: " + e.getMessage());
        }
    }
}
