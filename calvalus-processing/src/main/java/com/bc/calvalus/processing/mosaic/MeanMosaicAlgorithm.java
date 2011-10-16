package com.bc.calvalus.processing.mosaic;

import java.util.Arrays;

/**
 * A very simple algorithm, that computes the mean for all supplied bands.
 *
 * @author MarcoZ
 */
public class MeanMosaicAlgorithm implements MosaicAlgorithm {
    private float[][] aggregatedSamples = null;
    private int[][] counters = null;

    @Override
    public void process(float[][] samples) {
        int numBands = samples.length;
        if (aggregatedSamples == null) {
            int numElems = samples[0].length;
            aggregatedSamples = new float[numBands][numElems];
            counters = new int[numBands][numElems];
            for (int i = 0; i < numElems; i++) {
                Arrays.fill(aggregatedSamples[i], 0.0f);
                Arrays.fill(counters[i], 0);
            }
        }
        for (int i = 0; i < numBands; i++) {
            float[] aggregatedSample = aggregatedSamples[i];
            int[] counter = counters[i];
            float[] sample = samples[i];
            for (int j = 0; j < sample.length; j++) {
                float value = samples[i][j];
                if (!Float.isNaN(value)) {
                    aggregatedSample[j] += value;
                    counter[j]++;
                }
            }
        }
    }

    @Override
    public void finish() {
        for (int i = 0; i < aggregatedSamples.length; i++) {
            float[] aggregatedSample = aggregatedSamples[i];
            int[] counter = counters[i];
            for (int j = 0; j < aggregatedSample.length; j++) {
                int count = counter[j];
                if (count > 0) {
                    aggregatedSample[j] /= count;
                }
            }
        }
    }

    @Override
    public float[][] getResult() {
        return aggregatedSamples;
    }
}
