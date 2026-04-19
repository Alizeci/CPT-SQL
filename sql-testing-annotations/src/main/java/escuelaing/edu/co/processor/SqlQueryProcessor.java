package escuelaing.edu.co.processor;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

import escuelaing.edu.co.processor.annotation.Req;
import escuelaing.edu.co.processor.annotation.SqlQuery;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * CPT-SQL Phase 1: extracts {@link SqlQuery} + {@link Req} contracts at compile time
 * and serializes them into {@code loadtest/queries.json}.
 *
 * <p>Class-level {@code @Req} applies to all {@code @SqlQuery} methods in the class;
 * method-level {@code @Req} takes precedence. A duplicate {@code queryId} or a write
 * failure halt compilation with {@code ERROR}.</p>
 */
@SupportedAnnotationTypes({
    "escuelaing.edu.co.processor.annotation.SqlQuery",
    "escuelaing.edu.co.processor.annotation.Req"
})
@SupportedSourceVersion(SourceVersion.RELEASE_17)
public class SqlQueryProcessor extends AbstractProcessor {

    @Override
    public boolean process(Set<? extends TypeElement> annotations,
                           RoundEnvironment roundEnv) {

        if (roundEnv.processingOver()) return false;

        List<String> entries = new ArrayList<>();
        Set<String> seenQueryIds = new HashSet<>();

        for (Element element : roundEnv.getElementsAnnotatedWith(SqlQuery.class)) {
            if (element.getKind() != ElementKind.METHOD) continue;

            ExecutableElement method = (ExecutableElement) element;
            TypeElement enclosingClass = (TypeElement) method.getEnclosingElement();
            SqlQuery sqlQuery = method.getAnnotation(SqlQuery.class);

            // Duplicate queryId — ambiguous contract, halts compilation
            String queryId = sqlQuery.queryId();
            if (!seenQueryIds.add(queryId)) {
                processingEnv.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    "[LoadTest] Duplicate queryId: '" + queryId + "'. " +
                    "Each @SqlQuery method must have a unique queryId - " +
                    "pipeline artifacts (queries.json, baseline.json, load-profile.json) " +
                    "use queryId as the traceability key.",
                    method
                );
                continue;
            }

            // @Req: method level takes precedence over class level
            Req req = method.getAnnotation(Req.class);
            if (req == null) req = enclosingClass.getAnnotation(Req.class);

            // Incomplete contract — detector will skip P95_EXCEEDED and SLO_PROXIMITY
            if (req == null) {
                processingEnv.getMessager().printMessage(
                    Diagnostic.Kind.WARNING,
                    "[LoadTest] " + enclosingClass.getSimpleName() +
                    "#" + method.getSimpleName() +
                    " (queryId='" + queryId + "')" +
                    " has @SqlQuery without @Req - performance contract is not declared." +
                    " DegradationDetector will skip P95_EXCEEDED and SLO_PROXIMITY for this query.",
                    method
                );
            }

            entries.add(buildJsonEntry(
                queryId,
                enclosingClass.getQualifiedName().toString(),
                method.getSimpleName().toString(),
                sqlQuery.description(),
                req
            ));

            processingEnv.getMessager().printMessage(
                Diagnostic.Kind.NOTE,
                "[LoadTest] Processed: " + enclosingClass.getSimpleName() +
                "#" + method.getSimpleName() +
                " | queryId=" + queryId +
                (req != null ? " | sla=" + req.maxResponseTimeMs() + "ms" : " | sla=no contract")
            );
        }

        if (!entries.isEmpty()) {
            writeJson(entries);
        }
        return true;
    }

    private String buildJsonEntry(String queryId, String className,
                                   String methodName, String description,
                                   Req req) {
        boolean hasReq = req != null;
        return "    {\n" +
            "      \"queryId\": "          + jsonString(queryId)      + ",\n" +
            "      \"className\": "        + jsonString(className)    + ",\n" +
            "      \"methodName\": "       + jsonString(methodName)   + ",\n" +
            "      \"queryDescription\": " + jsonString(description)  + ",\n" +
            "      \"hasReq\": "           + hasReq                   + ",\n" +
            "      \"maxResponseTimeMs\": "+ (hasReq ? req.maxResponseTimeMs() : -1) + ",\n" +
            "      \"priority\": "         + jsonString(hasReq ? req.priority().name() : "NONE") + ",\n" +
            "      \"reqDescription\": "   + jsonString(hasReq ? req.description() : "") + ",\n" +
            "      \"allowPlanChange\": "  + (!hasReq || req.allowPlanChange()) + "\n" +
            "    }";
    }

    private void writeJson(List<String> entries) {
        try {
            FileObject resource = processingEnv.getFiler().createResource(
                StandardLocation.CLASS_OUTPUT, "",
                "loadtest/queries.json"
            );
            try (Writer writer = resource.openWriter()) {
                writer.write("{\n");
                writer.write("  \"generatedAt\": " + jsonString(java.time.Instant.now().toString()) + ",\n");
                writer.write("  \"totalQueries\": " + entries.size() + ",\n");
                writer.write("  \"queries\": [\n");
                for (int i = 0; i < entries.size(); i++) {
                    writer.write(entries.get(i));
                    if (i < entries.size() - 1) writer.write(",");
                    writer.write("\n");
                }
                writer.write("  ]\n}\n");
            }
            processingEnv.getMessager().printMessage(
                Diagnostic.Kind.NOTE,
                "[LoadTest] queries.json written to: " + resource.toUri()
            );
        } catch (IOException e) {
            processingEnv.getMessager().printMessage(
                Diagnostic.Kind.ERROR,
                "[LoadTest] Failed to write queries.json: " + e.getMessage() +
                " - pipeline phases 2-4 will not be able to run."
            );
        }
    }

    /** Escapes {@code value} for safe use as a JSON string. */
    private static String jsonString(String value) {
        if (value == null) return "\"\"";
        return "\"" + value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
            + "\"";
    }
}
