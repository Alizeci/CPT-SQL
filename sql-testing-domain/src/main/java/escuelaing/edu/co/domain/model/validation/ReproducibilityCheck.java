package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * Reproducibility check: two mirror generations with the same seed must produce
 * byte-identical data. Equality is verified by XOR-combining per-table CRC32 checksums.
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
