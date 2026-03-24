package escuelaing.edu.co.demo.ui;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * App demo del catálogo e-commerce — punto de entrada independiente del benchmark.
 *
 * <p>Levanta un servidor HTTP en el puerto 8080 con la pantalla del catálogo
 * de productos. Usa la query degradada para simular la experiencia del usuario
 * antes de que existiera el pipeline de pruebas de carga CPT-SQL.</p>
 *
 * <h3>Cómo ejecutar</h3>
 * <pre>
 * ./gradlew :sql-testing-demo:runDemo
 * </pre>
 * Luego abrir: http://localhost:8080
 */
@SpringBootApplication(scanBasePackages = "escuelaing.edu.co.demo.ui")
public class EcommerceDemoApp {

    public static void main(String[] args) {
        SpringApplication.run(EcommerceDemoApp.class, args);
    }
}
