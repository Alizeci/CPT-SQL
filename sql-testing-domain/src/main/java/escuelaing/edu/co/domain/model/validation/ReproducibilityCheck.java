package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * Reproducibility check: two runs with the same seed must produce byte-identical data.
 * Equality is verified via CRC32 over all generated table contents.
 */
@Data
@Builder
public class ReproducibilityCheck {

    private long seed;
    private long checksum1;
    private long checksum2;
    private boolean byteIdentical;
    private boolean pass;
}
