package com.bc.calvalus.processing.fire.format.grid.s2;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class S2GridMapperTest {

    @Test
    public void getErrorPerPixel() throws Exception {
        double[] probs = {
                0.56536696, 0.78055542, 0.65436347, 0.49271366, 0.76923176,
                0.49461837, 0.9761726, 0.38753145, 0.66194844, 0.73673713,
                0.8715599, 0.59177441, 0.19985001, 0.28629937, 0.70381885,
                0.98793774, 0.69215503, 0.27554467, 0.86892036, 0.64182099,
                0.29181729, 0.36298165, 0.7607316, 0.17223196, 0.86198424,
                0.1708366, 0.51868964, 0.02689661, 0.84591583, 0.76678544,
                0.39665975, 0.80400718, 0.06069604, 0.48492104, 0.13077186,
                0.36312609, 0.08083608, 0.22875986, 0.34459695, 0.32392777,
                0.25347801, 0.79042851, 0.60896279, 0.97076222, 0.64707434,
                0.62816483, 0.9933599, 0.80350037, 0.60894003, 0.7523323,
                0.26354999, 0.65738439, 0.12251649, 0.87043311, 0.15280995,
                0.20102399, 0.4567852, 0.19667328, 0.54721173, 0.76162707,
                0.47906019, 0.0786606, 0.33313626, 0.96322023, 0.61135772,
                0.11957433, 0.61950483, 0.37935909, 0.48772916, 0.29030689,
                0.88138267, 0.73888071, 0.20758538, 0.19298296, 0.9168182,
                0.58207931, 0.75525455, 0.9359131, 0.86915467, 0.1297407,
                0.45974684, 0.76526864, 0.56430141, 0.03041237, 0.65969498,
                0.62279958, 0.59339677, 0.9867703, 0.20068683, 0.88867341,
                0.0306582, 0.77244542, 0.80275072, 0.01832522, 0.30206282,
                0.93967375, 0.83246437, 0.06709965, 0.37869067, 0.1504346
        };
        double[] areas = new double[probs.length];
        Arrays.fill(areas, 0.5);
        assertEquals(4.0988349088, new S2GridMapper().getErrorPerPixel(probs, 0.5, 1), 1E-6);

    }

    @Test
    public void testGetErrorPerPixelNoProbs() throws Exception {
        double[] probs = new double[1000];
        Arrays.fill(probs, 0);
        double[] areas = new double[probs.length];
        Arrays.fill(areas, 0.5);
        assertEquals(0, new S2GridMapper().getErrorPerPixel(probs, 0.5, 1), 1E-6);
    }

    @Test
    public void testGetErrorPerPixelNaN() throws Exception {
        double[] probs = new double[5];
        probs[0] = 0;
        probs[1] = 0.5;
        probs[2] = 0.2;
        probs[3] = 0.534;
        probs[4] = 0.51;
        double[] areas = new double[probs.length];
        Arrays.fill(areas, 0.5);
        assertEquals(1.065800189, new S2GridMapper().getErrorPerPixel(probs, 0.5, 1), 1E-6);
        probs = new double[6];
        probs[0] = 0;
        probs[1] = 0.5;
        probs[2] = 0.2;
        probs[3] = 0.534;
        probs[4] = 0.51;
        probs[5] = Float.NaN;
        areas = new double[probs.length];
        Arrays.fill(areas, 0.5);
        assertEquals(1.065800189, new S2GridMapper().getErrorPerPixel(probs, 0.5, 1), 1E-6);
    }

    @Test
    public void testGetErrorPerPixelZero() throws Exception {
        double[] probs = new double[5];
        probs[0] = 0;
        probs[1] = 0.5;
        probs[2] = 0.2;
        probs[3] = 0.534;
        probs[4] = 0.51;
        double[] areas = new double[probs.length];
        Arrays.fill(areas, 0.5);
        assertEquals(1.0658, new S2GridMapper().getErrorPerPixel(probs, 0.5, 1), 1E-5);
    }

    @Test
    public void testGetErrorPerPixelInf() throws Exception {
        double[] probs = new double[1];
        probs[0] = 0.3;
        double[] areas = new double[probs.length];
        Arrays.fill(areas, 0.5);
        assertEquals(1.0, new S2GridMapper().getErrorPerPixel(probs, 0.5, 1), 1E-6);
    }
}