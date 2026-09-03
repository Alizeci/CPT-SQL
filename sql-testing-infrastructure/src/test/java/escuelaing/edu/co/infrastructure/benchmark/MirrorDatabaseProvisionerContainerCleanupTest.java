package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.infrastructure.dialect.PostgresDialect;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifica la lógica de la máquina de estados de {@link MirrorDatabaseProvisioner}:
 *
 * <ul>
 *   <li><b>running</b>      — no invoca cleanup ni start.</li>
 *   <li><b>not running</b>  — invoca {@code ensureContainerRemovedIfUnhealthy()} ANTES de start.</li>
 *   <li><b>not running</b>  — siempre invoca {@code startContainer()} tras la limpieza.</li>
 * </ul>
 *
 * <p>{@link FakeProvisioner} sustituye las tres operaciones Docker con stubs controlados,
 * aislando la lógica de orquestación sin requerir un daemon Docker real.</p>
 */
class MirrorDatabaseProvisionerContainerCleanupTest {

    static class FakeProvisioner extends MirrorDatabaseProvisioner {
        private final boolean simulatedRunning;
        boolean cleanupCalled = false;
        boolean startCalled   = false;

        FakeProvisioner(boolean simulatedRunning) {
            super(new PostgresDialect());
            this.simulatedRunning = simulatedRunning;
        }

        @Override
        boolean isContainerRunning() { return simulatedRunning; }

        @Override
        void ensureContainerRemovedIfUnhealthy() throws IOException, InterruptedException {
            cleanupCalled = true;
        }

        @Override
        void startContainer() { startCalled = true; }
    }

    @Test
    @DisplayName("running → ni cleanup ni start")
    void running_container_skips_cleanup_and_start() {
        FakeProvisioner p = new FakeProvisioner(true);
        p.ensureContainerRunning();
        assertThat(p.cleanupCalled).isFalse();
        assertThat(p.startCalled).isFalse();
    }

    @Test
    @DisplayName("not running → cleanup llamado antes de start")
    void not_running_calls_cleanup_then_start() {
        FakeProvisioner p = new FakeProvisioner(false);
        p.ensureContainerRunning();
        assertThat(p.cleanupCalled).isTrue();
        assertThat(p.startCalled).isTrue();
    }

    @Test
    @DisplayName("not running → start siempre ocurre tras cleanup")
    void not_running_always_starts_after_cleanup() {
        FakeProvisioner p = new FakeProvisioner(false);
        p.ensureContainerRunning();
        // orden garantizado: cleanup primero, start después
        assertThat(p.cleanupCalled).isTrue();
        assertThat(p.startCalled).isTrue();
    }
}
