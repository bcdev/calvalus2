package com.bc.calvalus.processing.mosaic;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.binning.VariableContext;

import java.util.Arrays;

/**
 * A very simple algorithm, that computes the mean for all supplied bands.
 *
 * @author MarcoZ
 */
public class MeanMosaicAlgorithm implements MosaicAlgorithm, Configurable {
    private float[][] aggregatedSamples = null;
    private int[][] counters = null;
    private String[] featureNames;
    private int variableCount;
    private int tileSize;
    private Configuration jobConf;

    @Override
    public void initTemporal(TileIndexWritable tileIndex) {
        int numElems = tileSize * tileSize;
        aggregatedSamples = new float[variableCount][numElems];
        counters = new int[variableCount][numElems];
        for (int band = 0; band < variableCount; band++) {
            Arrays.fill(aggregatedSamples[band], 0.0f);
            Arrays.fill(counters[band], 0);
        }
    }

    @Override
    public void processTemporal(float[][] samples) {
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
    public float[][] getTemporalResult() {
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
        featureNames = new String[variableCount];
        for (int i = 0; i < featureNames.length; i++) {
            featureNames[i] = variableContext.getVariableName(i) + "_mean";
        }
        tileSize = MosaicGrid.create(jobConf).getTileSize();
    }

    @Override
    public String[] getTemporalFeatures() {
        return featureNames;
    }

    @Override
    public float[][] getOutputResult(float[][] temporalData) {
        return temporalData;
    }

    @Override
    public String[] getOutputFeatures() {
        return featureNames;
    }

    @Override
    public void setConf(Configuration conf) {
        this.jobConf = conf;
    }

    @Override
    public Configuration getConf() {
        return jobConf;
    }

    @Override
    public MosaicProductFactory getProductFactory() {
        return new DefaultMosaicProductFactory(getTemporalFeatures());
    }
}
