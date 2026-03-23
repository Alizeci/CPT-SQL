package escuelaing.edu.co.domain.model.validation;

/**
 * Excepción lanzada cuando la validación de fidelidad del generador de datos
 * sintéticos no alcanza los umbrales de aceptación definidos.
 *
 * <p>Al ser lanzada en el pipeline CI/CD, provoca que el paso de benchmark falle
 * antes de ejecutar las consultas sobre datos que no son estadísticamente
 * representativos del tráfico de producción.</p>
 */
public class ValidationFailedException extends RuntimeException {

    private final ValidationReport report;

    public ValidationFailedException(ValidationReport report) {
        super("Validación de fidelidad fallida — estado: " + report.getValidationStatus());
        this.report = report;
    }

    public ValidationReport getReport() {
        return report;
    }
}
