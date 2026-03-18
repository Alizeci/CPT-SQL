package escuelaing.edu.co.processor.annotation;

import java.lang.annotation.*;

@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface SqlQuery {
    String queryId();
    String description() default "";
}