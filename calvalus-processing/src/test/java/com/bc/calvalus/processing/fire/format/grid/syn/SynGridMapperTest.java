package com.bc.calvalus.processing.fire.format.grid.syn;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class SynGridMapperTest {

    @Test
    public void getDotProduct() {
        double[][] Sx = new double[4][];
        Sx[0] = new double[]{13, 8, 6};
        Sx[1] = new double[]{9, 7, 4};
        Sx[2] = new double[]{7, 4, 0};
        Sx[3] = new double[]{15, 6, 3};
        double[] C = new double[]{3, 4, 2};
        double[] dotProduct = SynGridMapper.getDotProduct(C, Sx);
        assertArrayEquals(new double[]{83, 63, 37, 75}, dotProduct, 1E-6);
    }

    @Test
    public void getDotProduct_2() {
        double[][] Sx = new double[1][];
        Sx[0] = new double[]{4, 5, 6};
        double[] C = new double[]{1, 2, 3};
        double[] dotProduct = SynGridMapper.getDotProduct(C, Sx);
        assertArrayEquals(new double[]{32}, dotProduct, 1E-6);
    }

    @Test
    public void getDotProduct_3() {
        double[][] Sx = new double[4][];
        Sx[0] = new double[]{7, 5, 3, 4};
        Sx[1] = new double[]{4, 1, 4, 5};
        Sx[2] = new double[]{5, 3, 4, 3};
        Sx[3] = new double[]{3, 5, 6, 2};
        double[] C = new double[]{2, 3, 1, 2};
        double[] dotProduct = SynGridMapper.getDotProduct(C, Sx);
        assertArrayEquals(new double[]{40, 25, 29, 31}, dotProduct, 1E-6);
    }

    @Test
    public void testTranspose() {
        double[][] transposed = SynGridMapper.transpose(new double[]{1, 2, 3, 4, 5});
        double[][] expected = new double[1][];
        expected[0] = new double[]{1, 2, 3, 4, 5};
        assertArrayEquals(expected, transposed);
    }

    @Test
    public void getDotProduct_4() {
        double[][] Sx = new double[4][];
        Sx[0] = new double[]{7, 5, 3, 4};
        Sx[1] = new double[]{4, 1, 4, 5};
        Sx[2] = new double[]{5, 3, 4, 3};
        Sx[3] = new double[]{3, 5, 6, 2};

        double[] C = new double[]{5, 6, 8, 4};
        double[][] C_T = SynGridMapper.transpose(C);
        double[] V = SynGridMapper.getDotProduct(C, Sx);
        double[] result = SynGridMapper.getDotProduct(V, C_T);

        assertArrayEquals(new double[]{2093}, result, 1E-4);
    }

    @Test
    public void testGetProbabilityOfBurn() {
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
        final double[] doubles = Arrays.stream(probabilityOfBurn).map(v -> v / 100.0).toArray();
        assertEquals(1.134590, mapper.getErrorPerPixel(doubles, -1D, areas, -1D), 1E-4);
    }
}