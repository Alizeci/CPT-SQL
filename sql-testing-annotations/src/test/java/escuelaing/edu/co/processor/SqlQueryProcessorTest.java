package escuelaing.edu.co.processor;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.JavaFileObjects;
import org.junit.Test;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static com.google.testing.compile.Compiler.javac;

public class SqlQueryProcessorTest {

    @Test
    public void generatesQueriesJson_whenContractIsComplete() {
        Compilation compilation = javac()
            .withProcessors(new SqlQueryProcessor())
            .compile(JavaFileObjects.forSourceString(
                "com.example.ProductRepository",
                "import escuelaing.edu.co.processor.annotation.*;\n" +
                "public class ProductRepository {\n" +
                "    @SqlQuery(queryId = \"searchByCategory\", description = \"search\")\n" +
                "    @Req(maxResponseTimeMs = 300, allowPlanChange = false)\n" +
                "    public void searchByCategory(String cat) {}\n" +
                "}"
            ));

        assertThat(compilation).succeededWithoutWarnings();
        assertThat(compilation)
            .generatedFile(javax.tools.StandardLocation.CLASS_OUTPUT, "loadtest/queries.json")
            .contentsAsUtf8String()
            .contains("\"queryId\": \"searchByCategory\"");
    }

    @Test
    public void emitsWarning_whenReqIsMissing() {
        Compilation compilation = javac()
            .withProcessors(new SqlQueryProcessor())
            .compile(JavaFileObjects.forSourceString(
                "com.example.ProductRepository",
                "import escuelaing.edu.co.processor.annotation.*;\n" +
                "public class ProductRepository {\n" +
                "    @SqlQuery(queryId = \"noContract\")\n" +
                "    public void noContract() {}\n" +
                "}"
            ));

        assertThat(compilation).succeeded();
        assertThat(compilation)
            .hadWarningContaining("performance contract is not declared");
    }

    @Test
    public void emitsError_whenQueryIdIsDuplicated() {
        Compilation compilation = javac()
            .withProcessors(new SqlQueryProcessor())
            .compile(JavaFileObjects.forSourceString(
                "com.example.ProductRepository",
                "import escuelaing.edu.co.processor.annotation.*;\n" +
                "public class ProductRepository {\n" +
                "    @SqlQuery(queryId = \"sameId\")\n" +
                "    @Req(maxResponseTimeMs = 100)\n" +
                "    public void methodOne() {}\n" +
                "    @SqlQuery(queryId = \"sameId\")\n" +
                "    @Req(maxResponseTimeMs = 200)\n" +
                "    public void methodTwo() {}\n" +
                "}"
            ));

        assertThat(compilation).hadErrorContaining("Duplicate queryId");
    }

    @Test
    public void inheritsReq_whenDeclaredOnClass() {
        Compilation compilation = javac()
            .withProcessors(new SqlQueryProcessor())
            .compile(JavaFileObjects.forSourceString(
                "com.example.ProductRepository",
                "import escuelaing.edu.co.processor.annotation.*;\n" +
                "@Req(maxResponseTimeMs = 500)\n" +
                "public class ProductRepository {\n" +
                "    @SqlQuery(queryId = \"inheritedContract\")\n" +
                "    public void search() {}\n" +
                "}"
            ));

        assertThat(compilation).succeededWithoutWarnings();
        assertThat(compilation)
            .generatedFile(javax.tools.StandardLocation.CLASS_OUTPUT, "loadtest/queries.json")
            .contentsAsUtf8String()
            .contains("\"maxResponseTimeMs\": 500");
    }

    @Test
    public void escapesSpecialCharacters_inDescription() {
        Compilation compilation = javac()
            .withProcessors(new SqlQueryProcessor())
            .compile(JavaFileObjects.forSourceString(
                "com.example.ProductRepository",
                "import escuelaing.edu.co.processor.annotation.*;\n" +
                "public class ProductRepository {\n" +
                "    @SqlQuery(queryId = \"q1\", description = \"finds \\\"active\\\" products\")\n" +
                "    @Req(maxResponseTimeMs = 100, description = \"index: idx_products_active\")\n" +
                "    public void search() {}\n" +
                "}"
            ));

        assertThat(compilation).succeeded();
        assertThat(compilation)
            .generatedFile(javax.tools.StandardLocation.CLASS_OUTPUT, "loadtest/queries.json")
            .contentsAsUtf8String()
            .contains("\"queryId\": \"q1\"");
    }

    @Test
    public void methodReqOverridesClassReq_whenBothAreDeclared() {
        Compilation compilation = javac()
            .withProcessors(new SqlQueryProcessor())
            .compile(JavaFileObjects.forSourceString(
                "com.example.ProductRepository",
                "import escuelaing.edu.co.processor.annotation.*;\n" +
                "@Req(maxResponseTimeMs = 500)\n" +
                "public class ProductRepository {\n" +
                "    @SqlQuery(queryId = \"overridden\")\n" +
                "    @Req(maxResponseTimeMs = 100)\n" +
                "    public void search() {}\n" +
                "}"
            ));

        assertThat(compilation).succeededWithoutWarnings();
        assertThat(compilation)
            .generatedFile(javax.tools.StandardLocation.CLASS_OUTPUT, "loadtest/queries.json")
            .contentsAsUtf8String()
            .contains("\"maxResponseTimeMs\": 100");
    }
}
