package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Map;


/**
 * Consolidated fidelity validation report for the synthetic data generator.
 *
 * <p>Aggregates three dimensions: latency fidelity (error &lt; 10 %), cardinality
 * fidelity (error &lt; 5 %), and reproducibility (byte-identical across same seed).
 * Persisted as {@code VALIDATION_REPORT.json}.</p>
 */
@Data
@Builder
public class ValidationReport {

    public enum Status { PASS, FAIL }

    /** Overall verdict: PASS only if all three dimensions pass. */
    private Status validationStatus;

    private Instant generatedAt;
    private long seed;

    /** DPSDG parameters used in this generation run. */
    private SyntheticDataInfo syntheticDataGeneration;

    private FidelitySummary latencySummary;
    private Map<String, LatencyFidelity> latencyFidelity;
    private FidelitySummary cardinalitySummary;
    private Map<String, CardinalityFidelity> cardinalityFidelity;
    private ReproducibilityCheck reproducibility;

    /** {@code true} when {@code validationStatus == PASS}. */
    private boolean pass;

    private String notes;
}
