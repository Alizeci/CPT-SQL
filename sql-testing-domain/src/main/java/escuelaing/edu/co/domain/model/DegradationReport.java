package escuelaing.edu.co.domain.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.List;

/**
 * Informe de degradación generado por el {@code DegradationDetector} (Fase 3).
 *
 * <p>Detalla qué consultas fallaron, por qué tipo de degradación y cuáles
 * fueron los valores observados versus los umbrales de referencia.</p>
 *
 * <p>Es el artefacto de decisión que el pipeline CI/CD usa para bloquear
 * o aprobar un merge.</p>
 */
@Data
@Builder
public class DegradationReport {

    /** Momento en que se generó el informe. */
    private Instant generatedAt;

    /** Nombre del perfil de carga que originó este informe. */
    private String profileName;

    /** SHA del commit evaluado. */
    private String commitSha;

    /** {@code true} si existe al menos una degradación que bloquea el merge. */
    private boolean hasDegradations;

    /** Lista de degradaciones detectadas (vacía cuando {@code hasDegradations == false}). */
    private List<Degradation> degradations;

    // -------------------------------------------------------------------------

    /** Tipo de degradación detectada. */
    public enum DegradationType {

        /** El p95 medido supera el {@code maxResponseTimeMs} declarado en {@code @Req}. */
        P95_EXCEEDED,

        /**
         * El plan de ejecución cambió y la consulta tiene {@code allowPlanChange = false}
         * en {@code @Req}.
         */
        PLAN_CHANGED,

        /**
         * El p95 medido supera el p95 de la línea base en más de un margen de tolerancia
         * (10 % por defecto).
         */
        BASELINE_EXCEEDED,

        /**
         * El p95 medido supera el umbral interno de proximidad al SLA (80 % por defecto).
         * Indica que la consulta está operando en zona de riesgo: una variación normal
         * del entorno podría cruzar el SLA en la próxima ejecución.
         * Bloquea el merge aunque el SLA aún no haya sido formalmente violado.
         */
        SLO_PROXIMITY
    }

    /**
     * Detalle de una degradación individual.
     */
    @Data
    @Builder
    public static class Degradation {

        /** Consulta que presentó la degradación. */
        private String queryId;

        /** Tipo de degradación. */
        private DegradationType type;

        /**
         * Valor observado en el benchmark.
         * <ul>
         *   <li>{@link DegradationType#P95_EXCEEDED} → p95 medido en ms</li>
         *   <li>{@link DegradationType#PLAN_CHANGED} → costo del nuevo plan</li>
         *   <li>{@link DegradationType#BASELINE_EXCEEDED} → p95 medido en ms</li>
         * </ul>
         */
        private double observedValue;

        /**
         * Umbral que se superó.
         * <ul>
         *   <li>{@link DegradationType#P95_EXCEEDED} → {@code maxResponseTimeMs} de {@code @Req}</li>
         *   <li>{@link DegradationType#PLAN_CHANGED} → costo del plan de la línea base</li>
         *   <li>{@link DegradationType#BASELINE_EXCEEDED} → p95 de la línea base × 1.10</li>
         * </ul>
         */
        private double thresholdValue;

        /** Mensaje legible para el desarrollador. */
        private String description;
    }
}
