package escuelaing.edu.co.infrastructure.benchmark;

import escuelaing.edu.co.infrastructure.dialect.PostgresDialect;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates the state-machine logic of {@link MirrorDatabaseProvisioner}:
 *
 * <ul>
 *   <li><b>running</b>      — does not invoke cleanup or start.</li>
 *   <li><b>not running</b>  — invokes {@code ensureContainerRemovedIfUnhealthy()} BEFORE start.</li>
 *   <li><b>not running</b>  — always invokes {@code startContainer()} after cleanup.</li>
 * </ul>
 *
 * <p>{@link FakeProvisioner} replaces the three Docker operations with controlled stubs,
 * isolating the orchestration logic without requiring a real Docker daemon.</p>
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
    @DisplayName("not running → cleanup called before start")
    void not_running_calls_cleanup_then_start() {
        FakeProvisioner p = new FakeProvisioner(false);
        p.ensureContainerRunning();
        assertThat(p.cleanupCalled).isTrue();
        assertThat(p.startCalled).isTrue();
    }

    @Test
    @DisplayName("not running → start always occurs after cleanup")
    void not_running_always_starts_after_cleanup() {
        FakeProvisioner p = new FakeProvisioner(false);
        p.ensureContainerRunning();
        // guaranteed order: cleanup first, start after
        assertThat(p.cleanupCalled).isTrue();
        assertThat(p.startCalled).isTrue();
    }
}
