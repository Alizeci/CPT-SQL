package escuelaing.edu.co.domain.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Load profile replay settings for the benchmark engine: phase sequence, target rate,
 * query mixture and data access distribution. Use {@link #fromName(String)} to resolve a
 * predefined profile from {@code loadtest.benchmark.testProfile}.
 */
@Data
@Builder
public class TestProfile {

    private String name;
    private String description;

    /**
     * Declared execution mode. Informational only: the benchmark engine always paces
     * iterations to each phase's {@code targetTps}.
     */
    private ExecutionMode executionMode;

    /** How integer query parameters are drawn: UNIFORM over all IDs, or ZIPF (hot rows). */
    private AccessDistribution accessDistribution;

    /** Zipf exponent α, used only when {@code accessDistribution == ZIPF}. Default 1.0. */
    @Builder.Default
    private double zipfAlpha = 1.0;

    /**
     * Think time in milliseconds. Informational only: the benchmark engine derives the
     * pause from {@code targetTps} and otherwise uses {@code loadtest.benchmark.thinkTimeMs}.
     */
    @Builder.Default
    private long thinkTimeMs = 100;

    /** Not read by the benchmark engine; measurement is controlled per {@link Phase}. */
    @Builder.Default
    private boolean measurement = true;

    /** Ordered sequence of phases; phases with {@code measurement=false} are warm-up. */
    private List<Phase> phases;

    public enum ExecutionMode { CLOSED_LOOP, OPEN_LOOP }

    public enum AccessDistribution { UNIFORM, ZIPF }

    /**
     * Which load profile queries a phase executes. READ_HEAVY and WRITE_HEAVY fall back to
     * all queries when the profile has none of the requested kind.
     */
    public enum MixturePreset {
        /** Every query in the load profile. */
        DEFAULT,
        /** Only {@code SELECT} queries. */
        READ_HEAVY,
        /** Only non-{@code SELECT} queries. */
        WRITE_HEAVY
    }

    /**
     * STEP jumps to {@code targetTps} immediately; LINEAR ramps from the previous phase's
     * target (0 for the first phase).
     */
    public enum ThroughputFunction { STEP, LINEAR }

    /** A single execution phase within a TestProfile (target rate, mixture, duration). */
    @Data
    @Builder
    public static class Phase {

        private String name;

        /**
         * Target rate in iterations per second; each iteration executes every selected query
         * once and then pauses {@code 1000 / targetTps} ms.
         */
        private int targetTps;

        private int durationSecs;

        @Builder.Default
        private MixturePreset mixturePreset = MixturePreset.DEFAULT;

        @Builder.Default
        private ThroughputFunction throughputFunction = ThroughputFunction.STEP;

        /** If true, samples from this phase are included in the BenchmarkResult; false = warm-up. */
        @Builder.Default
        private boolean measurement = false;
    }

    // Predefined Profiles

    /**
     * Lightweight per-commit profile: 10 s warm-up to stabilise the PostgreSQL buffer cache,
     * then 60 s measurement.
     */
    public static TestProfile light() {
        return TestProfile.builder()
                .name("light")
                .description("Per-commit pre-test: 10 s warm-up + 60 s measurement")
                .executionMode(ExecutionMode.CLOSED_LOOP)
                .accessDistribution(AccessDistribution.UNIFORM)
                .thinkTimeMs(150)
                .phases(List.of(
                        Phase.builder()
                                .name("warmup")
                                .targetTps(10)
                                .durationSecs(10)
                                .mixturePreset(MixturePreset.DEFAULT)
                                .throughputFunction(ThroughputFunction.STEP)
                                .measurement(false)
                                .build(),
                        Phase.builder()
                                .name("measure")
                                .targetTps(10)
                                .durationSecs(60)
                                .mixturePreset(MixturePreset.DEFAULT)
                                .throughputFunction(ThroughputFunction.STEP)
                                .measurement(true)
                                .build()
                ))
                .build();
    }

    /**
     * Standard nightly profile: linear ramp-up then 120 s sustained measurement. Mirror
     * accuracy validation is skipped for this profile.
     */
    public static TestProfile normal() {
        return TestProfile.builder()
                .name("normal")
                .description("Nightly: ramp-up + 120 s sustained measurement")
                .executionMode(ExecutionMode.CLOSED_LOOP)
                .accessDistribution(AccessDistribution.UNIFORM)
                .thinkTimeMs(100)
                .phases(List.of(
                        Phase.builder()
                                .name("ramp_up")
                                .targetTps(50)
                                .durationSecs(30)
                                .mixturePreset(MixturePreset.DEFAULT)
                                .throughputFunction(ThroughputFunction.LINEAR)
                                .measurement(false)
                                .build(),
                        Phase.builder()
                                .name("measure")
                                .targetTps(100)
                                .durationSecs(120)
                                .mixturePreset(MixturePreset.DEFAULT)
                                .throughputFunction(ThroughputFunction.STEP)
                                .measurement(true)
                                .build()
                ))
                .build();
    }

    /** Flash-sale spike profile: normal → 4× peak → recovery, with Zipf access distribution. */
    public static TestProfile peak() {
        return TestProfile.builder()
                .name("peak")
                .description("Flash sale: normal → 4× peak → recovery (Zipf α=1.0)")
                .executionMode(ExecutionMode.CLOSED_LOOP)
                .accessDistribution(AccessDistribution.ZIPF)
                .zipfAlpha(1.0)
                .thinkTimeMs(50)
                .phases(List.of(
                        Phase.builder()
                                .name("normal")
                                .targetTps(100)
                                .durationSecs(60)
                                .mixturePreset(MixturePreset.DEFAULT)
                                .throughputFunction(ThroughputFunction.STEP)
                                .measurement(false)
                                .build(),
                        Phase.builder()
                                .name("flash_sale")
                                .targetTps(400)
                                .durationSecs(60)
                                .mixturePreset(MixturePreset.WRITE_HEAVY)
                                .throughputFunction(ThroughputFunction.STEP)
                                .measurement(true)
                                .build(),
                        Phase.builder()
                                .name("recovery")
                                .targetTps(100)
                                .durationSecs(30)
                                .mixturePreset(MixturePreset.DEFAULT)
                                .throughputFunction(ThroughputFunction.LINEAR)
                                .measurement(true)
                                .build()
                ))
                .build();
    }

    /** High sustained load: exposes degradations that only appear under prolonged pressure. */
    public static TestProfile sustained() {
        return TestProfile.builder()
                .name("sustained")
                .description("High sustained load — 120 s constant pressure")
                .executionMode(ExecutionMode.CLOSED_LOOP)
                .accessDistribution(AccessDistribution.UNIFORM)
                .thinkTimeMs(25)
                .phases(List.of(
                        Phase.builder()
                                .name("ramp_up")
                                .targetTps(200)
                                .durationSecs(30)
                                .mixturePreset(MixturePreset.DEFAULT)
                                .throughputFunction(ThroughputFunction.LINEAR)
                                .measurement(false)
                                .build(),
                        Phase.builder()
                                .name("tunnel")
                                .targetTps(400)
                                .durationSecs(120)
                                .mixturePreset(MixturePreset.DEFAULT)
                                .throughputFunction(ThroughputFunction.STEP)
                                .measurement(true)
                                .build()
                ))
                .build();
    }

    /** Oscillating load: alternates high/low to detect jitter and instability under variable load. */
    public static TestProfile wave() {
        return TestProfile.builder()
                .name("wave")
                .description("Oscillating load: alternating high/low phases")
                .executionMode(ExecutionMode.CLOSED_LOOP)
                .accessDistribution(AccessDistribution.UNIFORM)
                .thinkTimeMs(100)
                .phases(List.of(
                        Phase.builder()
                                .name("low_1")
                                .targetTps(50)
                                .durationSecs(30)
                                .mixturePreset(MixturePreset.READ_HEAVY)
                                .throughputFunction(ThroughputFunction.STEP)
                                .measurement(false)
                                .build(),
                        Phase.builder()
                                .name("high_1")
                                .targetTps(300)
                                .durationSecs(30)
                                .mixturePreset(MixturePreset.WRITE_HEAVY)
                                .throughputFunction(ThroughputFunction.LINEAR)
                                .measurement(true)
                                .build(),
                        Phase.builder()
                                .name("low_2")
                                .targetTps(50)
                                .durationSecs(30)
                                .mixturePreset(MixturePreset.READ_HEAVY)
                                .throughputFunction(ThroughputFunction.LINEAR)
                                .measurement(true)
                                .build(),
                        Phase.builder()
                                .name("high_2")
                                .targetTps(300)
                                .durationSecs(30)
                                .mixturePreset(MixturePreset.WRITE_HEAVY)
                                .throughputFunction(ThroughputFunction.LINEAR)
                                .measurement(true)
                                .build()
                ))
                .build();
    }

    /**
     * Returns the predefined profile for {@code name} (case-insensitive); unknown names
     * resolve to {@link #normal()}. {@code name} must not be null.
     */
    public static TestProfile fromName(String name) {
        return switch (name.toLowerCase()) {
            case "light"     -> light();
            case "peak"      -> peak();
            case "sustained" -> sustained();
            case "wave"      -> wave();
            default          -> normal();
        };
    }
}
