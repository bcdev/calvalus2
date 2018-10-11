package com.bc.calvalus.processing.fire.format.grid.avhrr;

import org.esa.snap.core.dataop.resamp.Resampling;

class TestRaster implements Resampling.Raster {

    float[][] array = new float[][]{
            new float[]{10, 20, 30, 40, 50},
            new float[]{30, 20, 30, 20, 30},
            new float[]{10, 40, 20, 30, 70},
            new float[]{20, 30, 40, 60, 80},
            new float[]{10, 40, 10, 90, 70},
    };

    public int getWidth() {
        return array[0].length;
    }

    public int getHeight() {
        return array.length;
    }

    public float getSample(double x, double y) {
        return array[(int) y][(int) x];
    }

    public boolean getSamples(int[] x, int[] y, double[][] samples) {
        for (int i = 0; i < y.length; i++) {
            for (int j = 0; j < x.length; j++) {
                samples[i][j] = getSample(x[j], y[i]);
            }
        }
        return true;
    }
}