package escuelaing.edu.co.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import escuelaing.edu.co.domain.model.QueryEntry;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Lee el archivo {@code queries.json} generado por el annotation processor
 * en la Fase 1 y construye un mapa {@code queryId → QueryEntry} en memoria.
 *
 * <p>Este mapa es el puente entre la Fase 1 (instrumentación estática) y la
 * Fase 2 (captura dinámica): permite que {@link SamplingFilter} consulte las
 * reglas de negocio declaradas en {@code @Req} sin volver a parsear JSON en
 * cada ejecución de consulta.</p>
 *
 * <p>El archivo se busca en el classpath bajo la ruta
 * {@code loadtest/queries.json}. Si no se encuentra se registra una advertencia
 * y el registro queda vacío — la captura sigue funcionando pero sin reglas @Req.</p>
 */
@Component
public class QueryRegistryLoader {

    private static final Logger LOG = Logger.getLogger(QueryRegistryLoader.class.getName());
    private static final String QUERIES_JSON_PATH = "loadtest/queries.json";

    private final ObjectMapper mapper;
    private Map<String, QueryEntry> registry = Collections.emptyMap();

    public QueryRegistryLoader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @PostConstruct
    public void load() {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(QUERIES_JSON_PATH);
        if (stream == null) {
            LOG.warning("[QueryRegistryLoader] No se encontró " + QUERIES_JSON_PATH +
                    " en el classpath. SamplingFilter operará sin reglas @Req.");
            return;
        }

        try {
            JsonNode root = mapper.readTree(stream);
            JsonNode queriesNode = root.path("queries");

            Map<String, QueryEntry> map = new HashMap<>();
            for (JsonNode q : queriesNode) {
                QueryEntry entry = QueryEntry.builder()
                        .queryId(q.path("queryId").asText())
                        .className(q.path("className").asText())
                        .methodName(q.path("methodName").asText())
                        .queryDescription(q.path("queryDescription").asText())
                        .hasReq(q.path("hasReq").asBoolean(false))
                        .maxResponseTimeMs(q.path("maxResponseTimeMs").asLong(1000L))
                        .priority(q.path("priority").asText("MEDIUM"))
                        .reqDescription(q.path("reqDescription").asText())
                        .allowPlanChange(q.path("allowPlanChange").asBoolean(true))
                        .build();
                map.put(entry.getQueryId(), entry);
            }

            this.registry = Collections.unmodifiableMap(map);
            LOG.info("[QueryRegistryLoader] Cargadas " + registry.size() + " entradas desde " + QUERIES_JSON_PATH);

        } catch (Exception e) {
            LOG.severe("[QueryRegistryLoader] Error al parsear " + QUERIES_JSON_PATH + ": " + e.getMessage());
        }
    }

    /** Devuelve el mapa completo {@code queryId → QueryEntry}. */
    public Map<String, QueryEntry> getRegistry() {
        return registry;
    }

    /** Devuelve la entrada para un queryId, o {@code null} si no existe. */
    public QueryEntry get(String queryId) {
        return registry.get(queryId);
    }
}
