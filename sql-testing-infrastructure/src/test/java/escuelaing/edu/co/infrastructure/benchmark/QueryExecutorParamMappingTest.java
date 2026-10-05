package escuelaing.edu.co.infrastructure.benchmark;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class QueryExecutorParamMappingTest {

    @Test
    @DisplayName("maps comparison and BETWEEN placeholders to their columns")
    void mapsSearchQuery() {
        List<QueryExecutor.ParamSlot> slots = QueryExecutor.mapParametersToColumns(
                "SELECT id FROM products WHERE active = true AND stock_quantity > 0 "
              + "AND category = ? AND price BETWEEN ? AND ? ORDER BY rating DESC LIMIT 20", 3);
        assertThat(slots).extracting(QueryExecutor.ParamSlot::column)
                .containsExactly("category", "price", "price");
        assertThat(slots).extracting(QueryExecutor.ParamSlot::role)
                .containsExactly(QueryExecutor.Role.COMPARE,
                                 QueryExecutor.Role.BETWEEN_LOW,
                                 QueryExecutor.Role.BETWEEN_HIGH);
    }

    @Test
    @DisplayName("maps increments, aliases and positional INSERT values")
    void mapsUpdateAndInsert() {
        assertThat(QueryExecutor.mapParametersToColumns(
                "UPDATE products SET stock_quantity = stock_quantity + ? WHERE id = ?", 2))
                .extracting(QueryExecutor.ParamSlot::column)
                .containsExactly("stock_quantity", "id");
        assertThat(QueryExecutor.mapParametersToColumns(
                "INSERT INTO orders(customer_id, status, total_amount) VALUES (?, 'PENDING', 0) RETURNING id", 1))
                .extracting(QueryExecutor.ParamSlot::column)
                .containsExactly("customer_id");
        assertThat(QueryExecutor.mapParametersToColumns(
                "SELECT 1 FROM products p WHERE p.category = ?", 1))
                .extracting(QueryExecutor.ParamSlot::column)
                .containsExactly("category");
    }
}
