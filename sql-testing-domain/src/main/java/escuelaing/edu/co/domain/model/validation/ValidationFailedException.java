package escuelaing.edu.co.domain.model.validation;

/** Thrown when synthetic data fidelity validation does not meet acceptance thresholds. */
public class ValidationFailedException extends RuntimeException {

    private final ValidationReport report;

    public ValidationFailedException(ValidationReport report) {
        super("Fidelity validation failed — status: " + report.getValidationStatus());
        this.report = report;
    }

    public ValidationReport getReport() {
        return report;
    }
}
