package escuelaing.edu.co.infrastructure.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
}
