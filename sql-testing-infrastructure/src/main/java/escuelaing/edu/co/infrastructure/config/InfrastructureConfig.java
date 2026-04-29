package escuelaing.edu.co.infrastructure.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import escuelaing.edu.co.infrastructure.capture.SanitizationStrategy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.Map;

/**
 * Central Spring configuration for the infrastructure module.
 *
 * <p>Defines shared beans used across capture, benchmark, and analysis
 * components. Centralising {@link ObjectMapper} here ensures all components
 * use the same Jackson configuration.</p>
 */
@Configuration
public class InfrastructureConfig {

    /**
     * Shared Jackson mapper: ISO-8601 timestamps, pretty-print enabled.
     * Injected into all components that need JSON serialisation or deserialisation.
     */
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .enable(SerializationFeature.INDENT_OUTPUT);
    }

    /**
     * No-op fallback used when the adopting application does not provide its own
     * {@link SanitizationStrategy}. Captures no data — safe by default.
     */
    @Bean
    @ConditionalOnMissingBean(SanitizationStrategy.class)
    public SanitizationStrategy defaultSanitizationStrategy() {
        return new SanitizationStrategy() {
            @Override public Map<String, Object> sanitize(ResultSet rs) { return Collections.emptyMap(); }
            @Override public Map<String, Object> sanitize(Map<String, Object> rawRow) { return Collections.emptyMap(); }
        };
    }
}
