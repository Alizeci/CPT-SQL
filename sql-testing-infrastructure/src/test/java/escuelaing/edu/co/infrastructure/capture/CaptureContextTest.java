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

    @Test
    void beginForced_setsForcedFlag_and_close_clearsIt() {
        assertThat(CaptureContext.isForced()).isFalse();
        try (CaptureContext ctx = CaptureContext.beginForced("q1")) {
            assertThat(CaptureContext.isForced()).isTrue();
            assertThat(CaptureContext.currentQueryId()).isEqualTo("q1");
        }
        assertThat(CaptureContext.isForced()).isFalse();
        assertThat(CaptureContext.currentQueryId()).isNull();
    }

    @Test
    void innerBegin_doesNotClearForcedFlag_setByOuterBeginForced() {
        try (CaptureContext outer = CaptureContext.beginForced("q1")) {
            try (CaptureContext inner = CaptureContext.begin("q1")) {
                assertThat(CaptureContext.isForced()).isTrue();
            }
            // inner closed — forced flag must still be true (outer owns it)
            assertThat(CaptureContext.isForced()).isTrue();
        }
        assertThat(CaptureContext.isForced()).isFalse();
    }

}
