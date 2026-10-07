package escuelaing.edu.co.domain.model.validation;

/** Thrown when a fidelity validation report has status "FAIL"; carries the full report. */
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
