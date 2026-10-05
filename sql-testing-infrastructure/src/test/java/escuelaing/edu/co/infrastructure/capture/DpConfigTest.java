package escuelaing.edu.co.infrastructure.capture;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DpConfigTest {

    @Test
    @DisplayName("declared categorical columns are parsed as table.column, lower-cased")
    void parseColumns_ok() {
        assertThat(DpConfig.parseColumns(" Products.Category , orders.status,,"))
                .containsExactly("products.category", "orders.status");
        assertThat(DpConfig.parseColumns("")).isEmpty();
    }

    @Test
    @DisplayName("a column without table prefix is rejected")
    void parseColumns_rejectsBareColumn() {
        assertThatThrownBy(() -> DpConfig.parseColumns("category"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("the 5-argument constructor keeps the default FK budget and no declared columns")
    void legacyConstructor_defaults() {
        DpConfig c = new DpConfig(DpConfig.parseBounds("price:0:10"), 0.6, 0.4, 1e-6, false);
        assertThat(c.epsilonForeignKey()).isEqualTo(DpConfig.DEFAULT_EPSILON_FK);
        assertThat(c.categoricalColumns()).isEmpty();
    }
}
