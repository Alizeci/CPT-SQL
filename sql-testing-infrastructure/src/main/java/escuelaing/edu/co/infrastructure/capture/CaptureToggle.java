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
 * Runtime switch for JDBC metric capture in the capture environment; no redeployment needed.
 *
 * <p>Controlled via JMX (MBean {@code escuelaing.edu.co:type=CaptureToggle}) or REST
 * ({@code GET /loadtest/capture}, {@code PUT /loadtest/capture?enabled=}). The flag is
 * {@code volatile}, so changes are visible to all threads without locking.</p>
 *
 * <p>Off by default so nothing accumulates on startup; set
 * {@code loadtest.capture.enabledOnStartup=true} to enable capture before the first
 * scheduled window.</p>
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

    /** REST control: {@code GET /loadtest/capture}, {@code PUT /loadtest/capture?enabled=}. */
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
