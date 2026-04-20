package escuelaing.edu.co.infrastructure.capture;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CaptureContextTest {

    @Test
    void begin_setsQueryIdOnCurrentThread() {
        try (CaptureContext ctx = CaptureContext.begin("searchProducts")) {
            assertThat(CaptureContext.currentQueryId()).isEqualTo("searchProducts");
        }
    }

    @Test
    void close_clearsQueryIdFromCurrentThread() {
        try (CaptureContext ctx = CaptureContext.begin("searchProducts")) {
            // inside scope
        }
        assertThat(CaptureContext.currentQueryId()).isNull();
    }

    @Test
    void currentQueryId_returnsNull_whenNoContextIsOpen() {
        assertThat(CaptureContext.currentQueryId()).isNull();
    }

}
