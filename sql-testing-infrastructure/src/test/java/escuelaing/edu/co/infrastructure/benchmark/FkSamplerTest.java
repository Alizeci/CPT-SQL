package escuelaing.edu.co.infrastructure.benchmark;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class FkSamplerTest {

    @Test
    @DisplayName("child rows follow the released concentration curve")
    void sampler_followsReleasedShares() {
        List<Integer> ids = new ArrayList<>();
        for (int i = 1; i <= 10_000; i++) ids.add(i);
        // top 1 % of parents own 50 % of rows, top 10 % own 80 %
        SyntheticDataGenerator.FkSampler s = SyntheticDataGenerator.FkSampler.of(
                ids, List.of(0.01, 0.10, 1.0), List.of(0.50, 0.80, 1.0));
        Random rng = new Random(42);
        int n = 200_000, top1 = 0, top10 = 0;
        for (int i = 0; i < n; i++) {
            int id = s.next(rng);
            if (id <= 100) top1++;
            if (id <= 1_000) top10++;
        }
        assertThat((double) top1 / n).isCloseTo(0.50, within(0.01));
        assertThat((double) top10 / n).isCloseTo(0.80, within(0.01));
    }

    @Test
    @DisplayName("a flat curve degenerates to uniform sampling")
    void sampler_uniformWhenFlat() {
        List<Integer> ids = List.of(1, 2, 3, 4);
        SyntheticDataGenerator.FkSampler s = SyntheticDataGenerator.FkSampler.of(
                ids, List.of(0.5, 1.0), List.of(0.5, 1.0));
        Random rng = new Random(1);
        int[] hits = new int[5];
        for (int i = 0; i < 40_000; i++) hits[s.next(rng)]++;
        for (int id = 1; id <= 4; id++) assertThat(hits[id] / 40_000.0).isCloseTo(0.25, within(0.02));
    }
}
