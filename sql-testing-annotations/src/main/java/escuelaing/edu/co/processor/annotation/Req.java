package escuelaing.edu.co.processor.annotation;

import java.lang.annotation.*;

/**
 * Declares the performance contract (SLA) of a {@link SqlQuery} method.
 *
 * <p>On a type, it applies to every {@code @SqlQuery} method in that type; a method-level
 * {@code @Req} takes precedence. A {@code @SqlQuery} method without any {@code @Req} has no
 * contract, so the gate skips {@code P95_EXCEEDED} and {@code SLO_PROXIMITY} for it.</p>
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface Req {

    /** Business priority of the query, reported alongside violations. */
    enum Priority { HIGH, MEDIUM, LOW }

    /** SLA on the p95 latency, in milliseconds. Must be positive. Default 1000. */
    long maxResponseTimeMs() default 1000L;
    /** Priority of the query. Default {@link Priority#MEDIUM}. */
    Priority priority() default Priority.MEDIUM;
    /** Free-text description of the requirement. */
    String description() default "";
    /**
     * Whether an execution plan change is acceptable. Default {@code true}; set {@code false}
     * to enable {@code PLAN_CHANGED} detection for this query.
     */
    boolean allowPlanChange() default true;
}
