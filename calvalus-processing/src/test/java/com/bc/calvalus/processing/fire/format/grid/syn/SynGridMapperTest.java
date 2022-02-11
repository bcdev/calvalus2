package com.bc.calvalus.processing.fire.format.grid.syn;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class SynGridMapperTest {

    @Test
    public void testGetProbabilityOfBurn() {
        SynGridMapper mapper = new SynGridMapper();
        double[] probabilityOfBurn = {
                2.05632, 1.15329, 1.26682, 1.52315, 1.26673,
                -1.0000, 3.18715, 2.84869, 1.61136, 2.03290,
                0.96077, -1.0000, -1.0000, -1.0000, 3.40598,
                1.59040, 1.30108, 3.31803, 1.29262, 1.22726,
                0.92350, 1.18129, 1.46975, 1.25647, 2.05632};
        double[] areas = {
                5, 5, 5, 5, 5,
                4, 4, 4, 4, 4,
                3, 3, 3, 3, 3,
                2, 2, 2, 2, 2,
                1, 1, 1, 1, 1
        };
        assertEquals(
                1.819140,
                mapper.getErrorPerPixel(probabilityOfBurn, -1D, areas, -1D),
                1E-4);
    }

    @Test
    public void testGetProbabilityOfBurn1() {
        SynGridMapper mapper = new SynGridMapper();
        double[] probabilityOfBurn = {
                2.05632, 1.15329, 1.26682, 1.52315, 1.26673,
                0.96077, 3.18715, 2.84869, 1.61136, 2.03290,
                -1.0000, -1.0000, -1.0000, -1.0000, 3.40598,
                1.59040, 1.30108, 3.31803, 1.29262, 1.22726,
                0.92350, 1.18129, 1.46975, 1.25647, 2.05632};
        double[] areas = {
                5, 5, 5, 5, 5,
                4, 4, 4, 4, 4,
                3, 3, 3, 3, 3,
                2, 2, 2, 2, 2,
                1, 1, 1, 1, 1
        };
        System.out.println(Arrays.stream(areas).average().getAsDouble());
        assertEquals(
                1.848481,
                mapper.getErrorPerPixel(probabilityOfBurn, -1D, areas, -1D),
                1E-4);
    }
}