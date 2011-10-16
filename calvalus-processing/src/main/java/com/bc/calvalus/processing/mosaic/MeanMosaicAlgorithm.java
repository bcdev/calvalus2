package com.bc.calvalus.processing.mosaic;

/**
* A very simple algorithm, that computes the mean for all supplied bands.
 *
 * @author MarcoZ
*/
public class MeanMosaicAlgorithm implements MosaicAlgorithm {
    private float[][] aggregatedSamples = null;
    private int counter = 0;

    @Override
    public void process(float[][] samples) {
        if (aggregatedSamples == null) {
            aggregatedSamples = new float[samples.length][samples[0].length];
        }
        for (int i = 0; i < samples.length; i++) {
            for (int j = 0; j < samples[i].length; j++) {
                aggregatedSamples[i][j] += samples[i][j];
            }
        }
        counter++;
    }

    @Override
    public void finish() {
        if (counter > 0) {
            for (int i = 0; i < aggregatedSamples.length; i++) {
                for (int j = 0; j < aggregatedSamples[i].length; j++) {
                    aggregatedSamples[i][j] /= counter;
                }
            }
        }
    }

    @Override
    public boolean hasResult() {
        return counter > 0;
    }

    @Override
    public float[][] getResult() {
        return aggregatedSamples;
    }
}
