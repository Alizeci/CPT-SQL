package escuelaing.edu.co.domain.model.validation;

import lombok.Builder;
import lombok.Data;

/**
 * Resultado de la verificación de reproducibilidad del generador de datos sintéticos.
 *
 * <p>Verifica que generar datos con el mismo {@code seed} produce exactamente los
 * mismos valores en dos ejecuciones consecutivas. La igualdad se comprueba con un
 * checksum CRC32 sobre el contenido de las tablas generadas.</p>
 *
 * <p>La reproducibilidad determinista es un requisito de SynQB (Liu et al., 2024)
 * para garantizar que los experimentos de benchmarking sean replicables.</p>
 */
@Data
@Builder
public class ReproducibilityCheck {

    /** Semilla usada en ambas generaciones. */
    private long seed;

    /** CRC32 de la primera generación. */
    private long checksum1;

    /** CRC32 de la segunda generación con la misma semilla. */
    private long checksum2;

    /** {@code true} si {@code checksum1 == checksum2}. */
    private boolean byteIdentical;

    /** {@code true} si la verificación fue exitosa (equivale a {@code byteIdentical}). */
    private boolean pass;
}
