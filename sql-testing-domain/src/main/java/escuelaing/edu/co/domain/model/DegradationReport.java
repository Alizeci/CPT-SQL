package escuelaing.edu.co.domain.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.List;

/**
 * Informe de degradación generado por el {@code DegradationDetector} (Fase 3).
 *
 * <p>Detalla qué consultas fallaron, por qué tipo de regresión y cuáles
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

    /** {@code true} si existe al menos una regresión que bloquea el merge. */
    private boolean hasRegressions;

    /** Lista de regresiones detectadas (vacía cuando {@code hasRegressions == false}). */
    private List<Regression> regressions;

    // -------------------------------------------------------------------------

    /** Tipo de regresión detectada. */
    public enum RegressionType {

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
        BASELINE_EXCEEDED
    }

    /**
     * Detalle de una regresión individual.
     */
    @Data
    @Builder
    public static class Regression {

        /** Consulta que presentó la regresión. */
        private String queryId;

        /** Tipo de regresión. */
        private RegressionType type;

        /**
         * Valor observado en el benchmark.
         * <ul>
         *   <li>{@link RegressionType#P95_EXCEEDED} → p95 medido en ms</li>
         *   <li>{@link RegressionType#PLAN_CHANGED} → costo del nuevo plan</li>
         *   <li>{@link RegressionType#BASELINE_EXCEEDED} → p95 medido en ms</li>
         * </ul>
         */
        private double observedValue;

        /**
         * Umbral que se superó.
         * <ul>
         *   <li>{@link RegressionType#P95_EXCEEDED} → {@code maxResponseTimeMs} de {@code @Req}</li>
         *   <li>{@link RegressionType#PLAN_CHANGED} → costo del plan de la línea base</li>
         *   <li>{@link RegressionType#BASELINE_EXCEEDED} → p95 de la línea base × 1.10</li>
         * </ul>
         */
        private double thresholdValue;

        /** Mensaje legible para el desarrollador. */
        private String description;
    }
}
