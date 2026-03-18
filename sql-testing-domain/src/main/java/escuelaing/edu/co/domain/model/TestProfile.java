package escuelaing.edu.co.domain.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Perfil de ejecución del motor de pruebas de carga (Fase 3).
 *
 * <p>Define <em>cómo</em> se ejecuta el benchmark sobre el {@link LoadProfile}:
 * cuánta carga, cómo varía en el tiempo y con qué distribución de acceso.
 * Complementa al {@code LoadProfile} (que define <em>qué</em> se ejecuta).</p>
 *
 * <h3>Inspiración en la literatura</h3>
 * <ul>
 *   <li><b>BenchPress (Van Aken et al., SIGMOD 2015) §2.1:</b> una fase se define
 *       como (1) target transaction rate, (2) transaction mixture, (3) duration.
 *       BenchPress añade control dinámico en tiempo real vía API REST.</li>
 *   <li><b>Dyn-YCSB (Sidhanta et al., IEEE SERVICES 2019):</b> el throughput
 *       objetivo varía según una función de tiempo {@code T = f(t)}, representada
 *       aquí como {@link ThroughputFunction} por fase.</li>
 * </ul>
 *
 * <h3>Perfiles predefinidos</h3>
 * <ul>
 *   <li>{@link #light()} — pre-test por commit: rápido, carga constante baja.</li>
 *   <li>{@link #normal()} — ejecución nocturna estándar.</li>
 *   <li>{@link #peak()} — simulación de pico esporádico (ej. flash sale).
 *       Equivale al challenge <em>Peak</em> de BenchPress §4.1.2.</li>
 *   <li>{@link #sustained()} — carga alta constante (BenchPress <em>Tunnel</em>).</li>
 *   <li>{@link #wave()} — carga oscilatoria (BenchPress <em>Sinusoidal</em>).</li>
 * </ul>
 *
 * <h3>Configuración en application.properties</h3>
 * <pre>
 * loadtest.testprofile=peak          # nombre del perfil predefinido
 * loadtest.benchmark.thinkTimeMs=100
 * </pre>
 */
@Data
@Builder
public class TestProfile {

    /** Nombre único del perfil (ej. "light", "peak", "nightly"). */
    private String name;

    /** Descripción legible para reportes y artefactos JSON. */
    private String description;

    /**
     * Modo de ejecución del loop de workers.
     *
     * <ul>
     *   <li>{@code CLOSED_LOOP}: throttled TPS — nunca supera el target aunque
     *       el DBMS pueda. Ideal para medir latencia bajo carga controlada
     *       (BenchPress §2.2.1).</li>
     *   <li>{@code OPEN_LOOP}: ejecuta tan rápido como sea posible — mide el
     *       techo real del DBMS. Útil en el challenge <em>Tunnel</em>.</li>
     * </ul>
     */
    private ExecutionMode executionMode;

    /**
     * Distribución de acceso a los datos en cada query.
     *
     * <ul>
     *   <li>{@code UNIFORM}: todos los IDs tienen la misma probabilidad.</li>
     *   <li>{@code ZIPF}: distribución sesgada — el top-20 % de los registros
     *       recibe ~80 % de los accesos (BenchPress §2 "time-evolving access skew").</li>
     * </ul>
     */
    private AccessDistribution accessDistribution;

    /**
     * Parámetro alfa de la distribución Zipf. Solo aplica cuando
     * {@code accessDistribution == ZIPF}. Valor típico: 1.0.
     */
    @Builder.Default
    private double zipfAlpha = 1.0;

    /**
     * Tiempo de espera entre transacciones, en milisegundos.
     * Simula el "think time" de un usuario real (BenchPress §2.2.1).
     */
    @Builder.Default
    private long thinkTimeMs = 100;

    /**
     * Indica si este perfil corresponde a la ventana de medición
     * (muestras se incluyen en el resultado) o solo a ramp-up/warm-up
     * (muestras descartadas). Los perfiles predefinidos marcan la última
     * fase como medición.
     */
    @Builder.Default
    private boolean measurement = true;

    /**
     * Secuencia de fases que componen este perfil.
     * Se ejecutan en orden. Las fases con {@code measurement=false} corresponden
     * al ramp-up; las fases con {@code measurement=true} aportan muestras al resultado.
     */
    private List<Phase> phases;

    // -------------------------------------------------------------------------
    // Enumeraciones
    // -------------------------------------------------------------------------

    public enum ExecutionMode { CLOSED_LOOP, OPEN_LOOP }

    public enum AccessDistribution { UNIFORM, ZIPF }

    public enum MixturePreset {
        /** Mezcla por defecto derivada del LoadProfile (~70% reads / 30% writes). */
        DEFAULT,
        /** Carga de solo lectura (BenchPress "read-only" preset). */
        READ_HEAVY,
        /** Carga intensiva de escritura (BenchPress "super-writes" preset). */
        WRITE_HEAVY
    }

    /**
     * Forma de la función de throughput dentro de una fase (Dyn-YCSB Fig. 1).
     *
     * <ul>
     *   <li>{@code STEP}: salto brusco al targetTps al inicio de la fase.</li>
     *   <li>{@code LINEAR}: rampa lineal desde el TPS de la fase anterior hasta targetTps.</li>
     * </ul>
     */
    public enum ThroughputFunction { STEP, LINEAR }

    // -------------------------------------------------------------------------
    // Fase individual
    // -------------------------------------------------------------------------

    /**
     * Una fase de ejecución dentro del TestProfile.
     *
     * <p>Equivale a la unidad mínima de BenchPress §2.1:
     * (target rate, mixture, duration).</p>
     */
    @Data
    @Builder
    public static class Phase {

        /** Nombre descriptivo de la fase (ej. "normal", "flash_sale", "recovery"). */
        private String name;

        /**
         * Throughput objetivo en operaciones por segundo.
         * En modo {@code OPEN_LOOP} este valor se ignora.
         */
        private int targetTps;

        /** Duración de la fase, en segundos. */
        private int durationSecs;

        /**
         * Preset de mezcla de transacciones para esta fase.
         * Controla la proporción reads/writes ejecutados.
         */
        @Builder.Default
        private MixturePreset mixturePreset = MixturePreset.DEFAULT;

        /**
         * Forma de la función T=f(t) para esta fase (Dyn-YCSB).
         * Define cómo el throughput transiciona desde la fase anterior.
         */
        @Builder.Default
        private ThroughputFunction throughputFunction = ThroughputFunction.STEP;

        /**
         * Si {@code true}, las muestras de esta fase se incluyen en el
         * {@link BenchmarkResult}. Si {@code false}, la fase es ramp-up.
         */
        @Builder.Default
        private boolean measurement = false;
    }

    // -------------------------------------------------------------------------
    // Perfiles predefinidos
    // -------------------------------------------------------------------------

    /**
     * Perfil ligero para pre-test por commit (Nivel 1 de CI/CD).
     *
     * <p>Carga constante baja durante 30 s. Retorno rápido compatible con
     * un ciclo de integración continua (Laaber et al., 2024).</p>
     */
    public static TestProfile light() {
        return TestProfile.builder()
                .name("light")
                .description("Pre-test por commit: carga constante baja (30 s)")
                .executionMode(ExecutionMode.CLOSED_LOOP)
                .accessDistribution(AccessDistribution.UNIFORM)
                .thinkTimeMs(150)
                .phases(List.of(
                        Phase.builder()
                                .name("measure")
                                .targetTps(10)
                                .durationSecs(30)
                                .mixturePreset(MixturePreset.DEFAULT)
                                .throughputFunction(ThroughputFunction.STEP)
                                .measurement(true)
                                .build()
                ))
                .build();
    }

    /**
     * Perfil estándar para ejecución nocturna completa (Nivel 2 de CI/CD).
     *
     * <p>Ramp-up lineal seguido de ventana de medición sostenida.
     * Equivale al challenge <em>Steps</em> de BenchPress §4.1.2.</p>
     */
    public static TestProfile normal() {
        return TestProfile.builder()
                .name("normal")
                .description("Nightly: ramp-up + medición sostenida (150 s)")
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

    /**
     * Perfil de pico esporádico — simula un evento de flash sale.
     *
     * <p>Equivale al challenge <em>Peak</em> de BenchPress §4.1.2:
     * "After a period of low throughput, a peak in throughput is created
     * for a short period before going back to normal."
     * La distribución Zipf concentra los accesos en los registros más
     * populares, simulando hot spots reales.</p>
     *
     * <p>El throughput sigue la función Step de Dyn-YCSB (Fig. 1a) entre
     * las fases normal → pico → recuperación.</p>
     */
    public static TestProfile peak() {
        return TestProfile.builder()
                .name("peak")
                .description("Flash sale: normal → pico ×4 → recovery (Zipf α=1.0)")
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

    /**
     * Perfil de carga alta sostenida.
     *
     * <p>Equivale al challenge <em>Tunnel</em> de BenchPress §4.1.2:
     * "long tunnels where the target execution is fixed to a constant range
     * of high target throughput." Útil para detectar degradaciones que solo
     * se manifiestan bajo carga sostenida prolongada.</p>
     */
    public static TestProfile sustained() {
        return TestProfile.builder()
                .name("sustained")
                .description("Carga alta constante sostenida — BenchPress Tunnel (120 s)")
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

    /**
     * Perfil de carga oscilatoria.
     *
     * <p>Equivale al challenge <em>Sinusoidal</em> de BenchPress §4.1.2:
     * "The character has to move up and down in a recurring pattern."
     * Alterna entre carga alta y baja para detectar jitter y falta de
     * estabilidad en el DBMS bajo carga variable.</p>
     */
    public static TestProfile wave() {
        return TestProfile.builder()
                .name("wave")
                .description("Carga oscilatoria alta/baja — BenchPress Sinusoidal")
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

    // -------------------------------------------------------------------------
    // Resolución por nombre
    // -------------------------------------------------------------------------

    /**
     * Retorna el perfil predefinido correspondiente al nombre dado.
     * Útil para cargar el perfil desde {@code application.properties}.
     *
     * @param name nombre del perfil ("light", "normal", "peak", "sustained", "wave")
     * @return el TestProfile predefinido, o {@code normal()} si no se reconoce el nombre
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
