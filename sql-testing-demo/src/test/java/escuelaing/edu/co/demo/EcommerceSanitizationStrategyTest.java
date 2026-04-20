package escuelaing.edu.co.demo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class EcommerceSanitizationStrategyTest {

    private EcommerceSanitizationStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = new EcommerceSanitizationStrategy();
    }

    @Test
    void sanitize_retainsOnlyWhitelistedColumns() {
        Map<String, Object> row = new HashMap<>();
        row.put("price",    29.99);
        row.put("email",    "user@example.com");
        row.put("category", "electronics");
        row.put("user_id",  42);

        Map<String, Object> result = strategy.sanitize(row);

        assertThat(result).containsKeys("price", "category");
        assertThat(result).doesNotContainKeys("email", "user_id");
    }

    @Test
    void sanitize_isCaseInsensitive() {
        Map<String, Object> row = Map.of("PRICE", 10.0, "EMAIL", "x@x.com");

        Map<String, Object> result = strategy.sanitize(row);

        assertThat(result).containsKey("price");
        assertThat(result).doesNotContainKey("email");
    }

    @Test
    void sanitize_emptyRow_returnsEmptyMap() {
        assertThat(strategy.sanitize(Map.of())).isEmpty();
    }

    @Test
    void sanitize_noWhitelistedColumns_returnsEmptyMap() {
        Map<String, Object> row = Map.of("email", "x@x.com", "phone", "123");
        assertThat(strategy.sanitize(row)).isEmpty();
    }

    @Test
    void sanitize_allWhitelistedColumns_areRetained() {
        Map<String, Object> row = new HashMap<>();
        EcommerceSanitizationStrategy.WHITELIST.forEach(col -> row.put(col, "value"));

        Map<String, Object> result = strategy.sanitize(row);

        assertThat(result.keySet()).containsExactlyInAnyOrderElementsOf(
                EcommerceSanitizationStrategy.WHITELIST);
    }
}
