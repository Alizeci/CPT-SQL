package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/** Aggregate summary for one fidelity dimension in the {@link ValidationReport}. */
@Data
@Builder
public class FidelitySummary {

    private int queriesTested;
    private int queriesPassed;
    private double meanErrorPct;
}
