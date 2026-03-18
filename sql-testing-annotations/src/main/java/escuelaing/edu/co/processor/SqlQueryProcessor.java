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
import java.util.List;
import java.util.Set;

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

        for (Element element : roundEnv.getElementsAnnotatedWith(SqlQuery.class)) {
            if (element.getKind() != ElementKind.METHOD) continue;

            ExecutableElement method = (ExecutableElement) element;
            TypeElement enclosingClass = (TypeElement) method.getEnclosingElement();

            SqlQuery sqlQuery = method.getAnnotation(SqlQuery.class);

            // @Req: método tiene precedencia sobre clase
            Req req = method.getAnnotation(Req.class);
            if (req == null) req = enclosingClass.getAnnotation(Req.class);

            String entry = buildJsonEntry(
                sqlQuery.queryId(),
                enclosingClass.getQualifiedName().toString(),
                method.getSimpleName().toString(),
                sqlQuery.description(),
                req
            );

            entries.add(entry);

            processingEnv.getMessager().printMessage(
                Diagnostic.Kind.NOTE,
                "[LoadTest] Procesado: " + enclosingClass.getSimpleName() +
                "#" + method.getSimpleName() +
                " | queryId=" + sqlQuery.queryId()
            );
        }

        if (!entries.isEmpty()) writeJson(entries);
        return true;
    }

    private String buildJsonEntry(String queryId, String className,
                                   String methodName, String description,
                                   Req req) {
        boolean hasReq = req != null;
        return "    {\n" +
            "      \"queryId\": \"" + queryId + "\",\n" +
            "      \"className\": \"" + className + "\",\n" +
            "      \"methodName\": \"" + methodName + "\",\n" +
            "      \"queryDescription\": \"" + description + "\",\n" +
            "      \"hasReq\": " + hasReq + ",\n" +
            "      \"maxResponseTimeMs\": " + (hasReq ? req.maxResponseTimeMs() : -1) + ",\n" +
            "      \"priority\": \"" + (hasReq ? req.priority().name() : "NONE") + "\",\n" +
            "      \"reqDescription\": \"" + (hasReq ? req.description() : "") + "\",\n" +
            "      \"allowPlanChange\": " + (!hasReq || req.allowPlanChange()) + "\n" +
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
                writer.write("  \"generatedAt\": \"" + java.time.Instant.now() + "\",\n");
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
                "[LoadTest] queries.json generado en: " + resource.toUri()
            );
        } catch (IOException e) {
            processingEnv.getMessager().printMessage(
                Diagnostic.Kind.ERROR,
                "[LoadTest] Error escribiendo queries.json: " + e.getMessage()
            );
        }
    }
}
