package escuelaing.edu.co.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import escuelaing.edu.co.domain.model.BenchmarkResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Gestiona la línea base de referencia del motor de benchmark (Fase 3).
 *
 * <h3>Línea base</h3>
 * <p>La línea base es el conjunto de métricas ({@link BenchmarkResult.QueryResult})
 * de la última ejecución aprobada, persistida en {@code baseline.json} en la
 * raíz del proyecto.  El {@link DegradationDetector} la usa para detectar
 * regresiones de tipo {@code BASELINE_EXCEEDED} y {@code PLAN_CHANGED}.</p>
 *
 * <h3>Versionado</h3>
 * <p>Cada actualización de la línea base escribe en {@code baseline.json}
 * sobreescribiendo el anterior.  El historial queda en el repositorio git,
 * de modo que cualquier versión anterior puede recuperarse con
 * {@code git show HEAD~n:baseline.json}.</p>
 *
 * <h3>Configuración (application.properties)</h3>
 * <pre>
 * loadtest.baseline.path=baseline.json
 * </pre>
 */
@Component
public class BaselineManager {

    private static final Logger LOG = Logger.getLogger(BaselineManager.class.getName());

    @Value("${loadtest.baseline.path:baseline.json}")
    private String baselinePath;

    private final ObjectMapper mapper;

    public BaselineManager() {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    // -------------------------------------------------------------------------
    // API pública
    // -------------------------------------------------------------------------

    /**
     * Lee la línea base almacenada y retorna un mapa {@code queryId →
     * QueryResult}.  Devuelve un mapa vacío si {@code baseline.json} no existe
     * aún (primera ejecución).
     *
     * @return mapa inmutable con la línea base actual
     */
    public Map<String, BenchmarkResult.QueryResult> load() {
        File file = new File(baselinePath);
        if (!file.exists()) {
            LOG.info("[BaselineManager] baseline.json no encontrado — primera ejecución.");
            return Collections.emptyMap();
        }
        try {
            BaselineFile baseline = mapper.readValue(file, BaselineFile.class);
            return baseline.queries != null
                    ? Collections.unmodifiableMap(baseline.queries)
                    : Collections.emptyMap();
        } catch (IOException e) {
            LOG.warning("[BaselineManager] No se pudo leer baseline.json: " + e.getMessage());
            return Collections.emptyMap();
        }
    }

    /**
     * Persiste el {@code result} como nueva línea base en {@code baseline.json}.
     * Solo debe llamarse cuando el benchmark produce un veredicto PASS.
     *
     * @param result resultado aprobado que se convierte en la nueva línea base
     */
    public void save(BenchmarkResult result) {
        BaselineFile baseline = new BaselineFile();
        baseline.commitSha = result.getCommitSha();
        baseline.profileName = result.getProfileName();
        baseline.savedAt = result.getExecutedAt().toString();
        baseline.queries = new HashMap<>(result.getQueries());

        try {
            mapper.writerWithDefaultPrettyPrinter().writeValue(new File(baselinePath), baseline);
            LOG.info("[BaselineManager] baseline.json actualizado (commit=" + result.getCommitSha() + ").");
        } catch (IOException e) {
            throw new RuntimeException("No se pudo guardar baseline.json", e);
        }
    }

    // -------------------------------------------------------------------------
    // Modelo de serialización
    // -------------------------------------------------------------------------

    /**
     * Estructura interna del archivo {@code baseline.json}.
     * Usa campos públicos para que Jackson pueda des/serializar sin
     * dependencias adicionales de configuración.
     */
    public static class BaselineFile {
        public String commitSha;
        public String profileName;
        public String savedAt;
        public Map<String, BenchmarkResult.QueryResult> queries;
    }
}
