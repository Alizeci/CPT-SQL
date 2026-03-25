package escuelaing.edu.co.infrastructure.capture;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
 * Tarea programada de Fase 2: abre una ventana de captura de tráfico real,
 * construye el {@link LoadProfile} y lo persiste como {@code load-profile.json}.
 *
 * <h3>Arquitectura</h3>
 * <p>En producción, el tráfico real ya fluye a través del {@link JdbcWrapper}.
 * Esta tarea solo administra <em>cuándo</em> se captura: habilita el
 * {@link CaptureToggle} al inicio de la ventana, espera {@code captureWindowSecs}
 * segundos y luego desactiva la captura y genera el perfil.</p>
 *
 * <p>En el ambiente demo (EC2 stand-in), el simulador {@code EcommerceSimulator}
 * ya corre con {@link JdbcWrapper} activo; basta con que esta tarea gestione
 * el toggle y la serialización.</p>
 *
 * <h3>Mecanismos de disparo</h3>
 * <ul>
 *   <li><b>Programado</b> — {@code @Scheduled} a las 2 AM UTC (cron configurable).</li>
 *   <li><b>Manual</b> — {@code POST /loadtest/phase2/run?windowSecs=N} para pruebas
 *       desde CI o desde GitHub Actions con {@code workflow_dispatch}.</li>
 * </ul>
 *
 * <h3>Configuración (application.properties)</h3>
 * <pre>
 * loadtest.phase2.cron=0 0 2 * * *
 * loadtest.phase2.captureWindowSecs=120
 * loadtest.phase2.outputPath=load-profile.json
 * </pre>
 */
@Component
public class Phase2ScheduledTask {

    private static final Logger LOG = Logger.getLogger(Phase2ScheduledTask.class.getName());

    private final CaptureToggle    captureToggle;
    private final LoadProfileBuilder profileBuilder;
    private final ObjectMapper     mapper;

    /** Evita ejecuciones solapadas si la ventana es más larga que el intervalo del cron. */
    private final AtomicBoolean running = new AtomicBoolean(false);

    @Value("${loadtest.phase2.captureWindowSecs:120}")
    private int captureWindowSecs;

    @Value("${loadtest.phase2.outputPath:load-profile.json}")
    private String outputPath;

    public Phase2ScheduledTask(CaptureToggle captureToggle,
                                LoadProfileBuilder profileBuilder) {
        this.captureToggle  = captureToggle;
        this.profileBuilder = profileBuilder;
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .enable(SerializationFeature.INDENT_OUTPUT);
    }

    // -------------------------------------------------------------------------
    // Disparo programado — 2 AM UTC (configurable)
    // -------------------------------------------------------------------------

    /**
     * Abre la ventana de captura nocturnamente.
     *
     * <p>El cron se configura en {@code application.properties} con la propiedad
     * {@code loadtest.phase2.cron}. Por defecto: {@code 0 0 2 * * *} (2 AM UTC).</p>
     *
     * <p>Si ya hay una ejecución en curso, el disparo se omite para evitar
     * solapamiento.</p>
     */
    @Scheduled(cron = "${loadtest.phase2.cron:0 0 2 * * *}")
    public void runScheduled() {
        LOG.info("[Phase2] Disparo programado (cron 2 AM UTC).");
        executeCaptureWindow(captureWindowSecs);
    }

    // -------------------------------------------------------------------------
    // Disparo manual — REST endpoint
    // -------------------------------------------------------------------------

    /**
     * Controlador REST que expone el disparo manual de la captura Fase 2.
     *
     * <ul>
     *   <li>{@code POST /loadtest/phase2/run} — usa {@code captureWindowSecs} por defecto.</li>
     *   <li>{@code POST /loadtest/phase2/run?windowSecs=60} — ventana personalizada.</li>
     * </ul>
     *
     * <p>Útil para pruebas desde CI/CD o para verificar el funcionamiento del
     * pipeline sin esperar al cron nocturno (equivalente a {@code workflow_dispatch}
     * en GitHub Actions).</p>
     */
    @RestController
    @RequestMapping("/loadtest/phase2")
    public class Controller {

        @PostMapping("/run")
        public Map<String, Object> runManual(
                @RequestParam(defaultValue = "0") int windowSecs) {
            int window = windowSecs > 0 ? windowSecs : captureWindowSecs;
            if (running.get()) {
                return Map.of(
                        "status", "SKIPPED",
                        "reason", "Ya hay una captura en curso.");
            }
            // Ejecutar en hilo separado para no bloquear la respuesta HTTP
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

    // -------------------------------------------------------------------------
    // Lógica de captura
    // -------------------------------------------------------------------------

    /**
     * Abre la ventana de captura por {@code windowSecs} segundos, construye el
     * {@link LoadProfile} y lo persiste en {@code outputPath}.
     *
     * <p>El método es idempotente respecto al toggle: restaura el estado previo
     * al finalizar (o en caso de error) para no interferir con otras partes del
     * sistema que puedan haber desactivado la captura intencionalmente.</p>
     *
     * @param windowSecs duración de la ventana de captura en segundos
     */
    void executeCaptureWindow(int windowSecs) {
        if (!running.compareAndSet(false, true)) {
            LOG.warning("[Phase2] Captura omitida — ya hay una ejecución en curso.");
            return;
        }

        boolean previousState = captureToggle.isEnabled();
        try {
            LOG.info(String.format("[Phase2] Abriendo ventana de captura (%d s)...", windowSecs));
            captureToggle.enable();

            Thread.sleep(windowSecs * 1_000L);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warning("[Phase2] Ventana de captura interrumpida.");
        } finally {
            captureToggle.setEnabled(previousState);
            running.set(false);
        }

        buildAndPersist();
    }

    /**
     * Drena el {@link MetricsBuffer} via {@link LoadProfileBuilder}, construye
     * el {@link LoadProfile} y lo serializa a {@code load-profile.json}.
     *
     * <p>Si el perfil está vacío (ninguna query fue capturada), se emite una
     * advertencia y no se sobreescribe el archivo previo.</p>
     */
    private void buildAndPersist() {
        LoadProfile profile = profileBuilder.build();

        if (profile.getQueries() == null || profile.getQueries().isEmpty()) {
            LOG.warning("[Phase2] Perfil vacío — no se capturó ninguna query. "
                    + "Verificar que JdbcWrapper esté activo y que haya tráfico.");
            return;
        }

        long nonZeroLatencies = profile.getQueries().values().stream()
                .filter(s -> s.getP95Ms() > 0)
                .count();
        if (nonZeroLatencies == 0) {
            LOG.warning("[Phase2] Todas las latencias son 0 ms — posible bug en JdbcWrapper.");
        }

        try {
            Path output = Path.of(outputPath);
            mapper.writeValue(output.toFile(), profile);
            LOG.info(String.format(
                    "[Phase2] load-profile.json generado: %d queries, ruta=%s",
                    profile.getQueries().size(), output.toAbsolutePath()));
        } catch (IOException e) {
            LOG.severe("[Phase2] Error al persistir load-profile.json: " + e.getMessage());
        }
    }
}
