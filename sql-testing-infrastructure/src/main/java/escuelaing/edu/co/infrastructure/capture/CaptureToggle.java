package escuelaing.edu.co.infrastructure.capture;

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
 * Feature flag que habilita o deshabilita la captura de métricas en caliente,
 * sin requerir un redespliegue de la aplicación.
 *
 * <h3>Mecanismos de control</h3>
 * <ul>
 *   <li><b>JMX</b> — a través del MBean {@code escuelaing.edu.co:type=CaptureToggle}.
 *       Se puede operar desde JConsole o cualquier cliente JMX.</li>
 *   <li><b>REST</b> — endpoint HTTP integrado en la misma clase (inner
 *       {@link Controller}) para control desde CI/CD o dashboards.</li>
 * </ul>
 *
 * <p>El flag es {@code volatile} para garantizar visibilidad entre hilos sin
 * necesidad de sincronización explícita.</p>
 */
@Component
@ManagedResource(
        objectName = "escuelaing.edu.co:type=CaptureToggle",
        description = "Habilita o deshabilita la captura de métricas JDBC en caliente"
)
public class CaptureToggle {

    private volatile boolean enabled = true;

    // -------------------------------------------------------------------------
    // API interna — usada por JdbcWrapper
    // -------------------------------------------------------------------------

    /** Retorna {@code true} si la captura está habilitada. */
    @ManagedAttribute(description = "Estado actual del flag de captura")
    public boolean isEnabled() {
        return enabled;
    }

    // -------------------------------------------------------------------------
    // Control vía JMX
    // -------------------------------------------------------------------------

    @ManagedAttribute(description = "Habilita o deshabilita la captura de métricas")
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @ManagedOperation(description = "Habilita la captura de métricas")
    public void enable() {
        this.enabled = true;
    }

    @ManagedOperation(description = "Deshabilita la captura de métricas")
    public void disable() {
        this.enabled = false;
    }

    // -------------------------------------------------------------------------
    // Control vía REST — endpoint anidado
    // -------------------------------------------------------------------------

    /**
     * Controlador REST que expone el toggle de captura.
     *
     * <ul>
     *   <li>{@code GET  /loadtest/capture} — devuelve el estado actual.</li>
     *   <li>{@code PUT  /loadtest/capture?enabled=true|false} — cambia el estado.</li>
     * </ul>
     */
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
