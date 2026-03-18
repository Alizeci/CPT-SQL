package escuelaing.edu.co.processor.annotation;

import java.lang.annotation.*;

@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface Req {

    enum Priority { HIGH, MEDIUM, LOW }

    long maxResponseTimeMs() default 1000L;
    Priority priority() default Priority.MEDIUM;
    String description() default "";
    boolean allowPlanChange() default true;
}