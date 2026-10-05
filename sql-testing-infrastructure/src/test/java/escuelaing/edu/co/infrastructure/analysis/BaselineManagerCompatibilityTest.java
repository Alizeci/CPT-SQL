package escuelaing.edu.co.infrastructure.analysis;

import escuelaing.edu.co.domain.model.BenchmarkResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BaselineManagerCompatibilityTest {

    private static BenchmarkResult result(String testProfile, long rows, String fingerprint) {
        return BenchmarkResult.builder()
                .profileName("demo").testProfileName(testProfile)
                .rowsPerTable(rows).loadProfileFingerprint(fingerprint)
                .executedAt(Instant.now()).commitSha("abc")
                .queries(Map.of())
                .build();
    }

    private static BaselineManager managerAt(Path dir) {
        BaselineManager m = new BaselineManager();
        ReflectionTestUtils.setField(m, "baselinePath", dir.resolve("baseline.json").toString());
        return m;
    }

    @Test
    @DisplayName("missing baseline is reported as MISSING")
    void missing(@TempDir Path dir) {
        assertThat(managerAt(dir).compatibilityWith(result("light", 400_000, "f1"))).isEqualTo("MISSING");
    }

    @Test
    @DisplayName("same TestProfile, volume and load profile → COMPARABLE")
    void comparable(@TempDir Path dir) {
        BaselineManager m = managerAt(dir);
        m.save(result("light", 400_000, "f1"));
        assertThat(m.compatibilityWith(result("light", 400_000, "f1"))).isEqualTo("COMPARABLE");
    }

    @Test
    @DisplayName("baseline measured with another TestProfile or load profile → INCOMPATIBLE")
    void incompatible(@TempDir Path dir) {
        BaselineManager m = managerAt(dir);
        m.save(result("normal", 400_000, "f1"));
        assertThat(m.compatibilityWith(result("light", 400_000, "f1")))
                .startsWith("INCOMPATIBLE").contains("testProfile normal != light");
        m.save(result("light", 400_000, "f1"));
        assertThat(m.compatibilityWith(result("light", 400_000, "f2")))
                .startsWith("INCOMPATIBLE").contains("load profile changed");
    }
}
