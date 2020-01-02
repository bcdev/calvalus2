package com.bc.calvalus.processing.fire.format.grid;

import org.esa.snap.core.dataop.resamp.Resampling;

import java.util.HashMap;
import java.util.Map;

public class Majority implements Resampling {

    private final static int KERNEL_SIZE = 19;
    private final static int HALF_KERNEL_SIZE = KERNEL_SIZE / 2;

    public String getName() {
        return "MAJORITY";
    }

    public final Index createIndex() {
        return new Index(KERNEL_SIZE, 1);
    }

    public final void computeIndex(final double x,
                                   final double y,
                                   final int width,
                                   final int height,
                                   final Index index) {
        index.x = x;
        index.y = y;
        index.width = width;
        index.height = height;

        final int i0 = (int) Math.floor(x);
        final int j0 = (int) Math.floor(y);

        final double di = x - (i0 + 0.5);
        final double dj = y - (j0 + 0.5);

        index.i0 = i0;
        index.j0 = j0;

        final int iMax = width - 1;
        final int jMax = height - 1;

        final int size = KERNEL_SIZE;
        final int minI = i0 - HALF_KERNEL_SIZE;
        final int minJ = j0 - HALF_KERNEL_SIZE;

        int v;
        if (di >= 0) {
            for (int i = 0; i < size; i++) {
                v = minI + i;
                index.i[i] = Math.min(v >= 0 ? v : 0, iMax);
            }
            index.ki[0] = di;
        } else {
            for (int i = 0; i < size; i++) {
                v = minI - 1 + i;
                index.i[i] = Math.min(v >= 0 ? v : 0, iMax);
            }
            index.ki[0] = di + 1;
        }

        if (dj >= 0) {
            for (int j = 0; j < size; j++) {
                v = minJ + j;
                index.j[j] = Math.min(v >= 0 ? v : 0, jMax);
            }
            index.kj[0] = dj;
        } else {
            for (int j = 0; j < size; j++) {
                v = minJ - 1 + j;
                index.j[j] = Math.min(v >= 0 ? v : 0, jMax);
            }
            index.kj[0] = dj + 1;
        }
    }

    public final double resample(final Raster raster,
                                 final Index index) throws Exception {

        final int size = KERNEL_SIZE;

        final int[] x = new int[size];
        final int[] y = new int[size];
        final double[][] samples = new double[size][size];

        for (int n = 0; n < size; n++) {
            x[n] = (int) index.i[n];
            y[n] = (int) index.j[n];
        }

        Map<Double, Integer> valueCountMap = new HashMap<>();
        raster.getSamples(x, y, samples);
        for (double[] doubles : samples) {
            for (double aDouble : doubles) {
                Double key = aDouble;
                // only consider burnable classes
                if (key < 10 || key > 180) {
                    continue;
                }
                if (valueCountMap.containsKey(key)) {
                    valueCountMap.put(key, valueCountMap.get(key) + 1);
                } else {
                    valueCountMap.put(key, 1);
                }
            }
        }

        Map.Entry<Double, Integer> maxEntry = null;
        for (Map.Entry<Double, Integer> entry : valueCountMap.entrySet()) {
            if (maxEntry == null || (entry.getValue() >= maxEntry.getValue())) {
                maxEntry = entry;
            }
        }

        if (maxEntry == null) {
            return 210.0; // unburnable
        }
        return maxEntry.getKey();
    }

    @Override
    public String toString() {
        return "Majority resampling";
    }
}

