package escuelaing.edu.co.processor.annotation;

import java.lang.annotation.*;

/**
 * Marks a repository method that executes a SQL query tracked by CPT-SQL. Pair it with
 * {@link Req} to declare its performance contract.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface SqlQuery {
    /**
     * Unique identifier of the query; the traceability key across queries.json, load-profile.json
     * and baseline.json. Must start with a letter and contain only letters, digits and
     * underscores; duplicates fail compilation.
     */
    String queryId();
    /** Free-text description of what the query does. */
    String description() default "";
}
