package com.bc.calvalus.processing.mosaic;

import com.bc.calvalus.binning.VariableContext;

import java.util.Arrays;

/**
 * A very simple algorithm, that computes the mean for all supplied bands.
 *
 * @author MarcoZ
 */
public class MeanMosaicAlgorithm implements MosaicAlgorithm {
    private float[][] aggregatedSamples = null;
    private int[][] counters = null;
    private String[] outputFeatures;
    private int variableCount;
    private int tileSize;

    @Override
    public void init() {
        int numElems = tileSize * tileSize;
        aggregatedSamples = new float[variableCount][numElems];
        counters = new int[variableCount][numElems];
        for (int band = 0; band < variableCount; band++) {
            Arrays.fill(aggregatedSamples[band], 0.0f);
            Arrays.fill(counters[band], 0);
        }
    }

    @Override
    public void process(float[][] samples) {
        for (int band = 0; band < variableCount; band++) {
            float[] aggregatedSample = aggregatedSamples[band];
            int[] counter = counters[band];
            float[] sample = samples[band];
            for (int i = 0; i < sample.length; i++) {
                float value = sample[i];
                if (!Float.isNaN(value)) {
                    aggregatedSample[i] += value;
                    counter[i]++;
                }
            }
        }
    }

    @Override
    public float[][] getResult() {
        for (int band = 0; band < aggregatedSamples.length; band++) {
            float[] aggregatedSample = aggregatedSamples[band];
            int[] counter = counters[band];
            for (int i = 0; i < aggregatedSample.length; i++) {
                int count = counter[i];
                if (count > 0) {
                    aggregatedSample[i] /= count;
                }
            }
        }
        return aggregatedSamples;
    }

    @Override
    public void setVariableContext(VariableContext variableContext) {
        variableCount = variableContext.getVariableCount();
        outputFeatures = new String[variableCount];
        for (int i = 0; i < outputFeatures.length; i++) {
            outputFeatures[i] = variableContext.getVariableName(i) + "_mean";
        }
        tileSize = new MosaicGrid().getTileSize();
    }

    @Override
    public String[] getOutputFeatures() {
        return outputFeatures;
    }
}
