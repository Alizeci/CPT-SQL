package escuelaing.edu.co.infrastructure.capture;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Feature flag that enables or disables metric capture at runtime without redeployment.
 *
 * <p>Exposed via JMX (MBean {@code escuelaing.edu.co:type=CaptureToggle}) and a
 * built-in REST endpoint for control from CI/CD or dashboards. The flag is
 * {@code volatile} to ensure cross-thread visibility without explicit synchronization.</p>
 *
 * <p>Defaults to {@code false} (capture off) to avoid unintended data accumulation
 * on startup. Set {@code loadtest.capture.enabledOnStartup=true} to pre-enable
 * capture before the first scheduled window.</p>
 *
 * <h3>Configuration</h3>
 * <pre>
 * loadtest.capture.enabledOnStartup=false
 * </pre>
 */
@Component
@ManagedResource(
        objectName = "escuelaing.edu.co:type=CaptureToggle",
        description = "Enables or disables JDBC metric capture at runtime"
)
public class CaptureToggle {

    @Value("${loadtest.capture.enabledOnStartup:false}")
    private boolean enabledOnStartup;

    private volatile boolean enabled;

    @PostConstruct
    public void init() {
        this.enabled = enabledOnStartup;
    }

    @ManagedAttribute(description = "Current capture flag state")
    public boolean isEnabled() {
        return enabled;
    }

    @ManagedAttribute(description = "Enable or disable metric capture")
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @ManagedOperation(description = "Enable metric capture")
    public void enable() {
        this.enabled = true;
    }

    @ManagedOperation(description = "Disable metric capture")
    public void disable() {
        this.enabled = false;
    }

    // REST control — GET /loadtest/capture, PUT /loadtest/capture?enabled=

    @RestController
    @RequestMapping("/loadtest/capture")
    public class Controller {

        @GetMapping
        public Map<String, Object> status() {
            return Map.of("captureEnabled", enabled);
        }

        @PutMapping
        public Map<String, Object> toggle(@RequestParam boolean enabled) {
            CaptureToggle.this.enabled = enabled;
            return Map.of("captureEnabled", CaptureToggle.this.enabled);
        }
    }
}
