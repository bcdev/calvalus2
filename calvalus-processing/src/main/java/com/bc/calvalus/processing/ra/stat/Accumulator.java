package com.bc.calvalus.processing.ra.stat;

/**
 * accumulates 'valid' float values
 */
class Accumulator {

    private float[] values;

    public Accumulator() {
        values = new float[0];
    }

    public void accumulateNoNaN(float... samples) {
        float[] buffer = new float[samples.length];
        int i = 0;
        for (float sample : samples) {
            if (!Float.isNaN(sample)) {
                buffer[i++] = sample;
            }
        }
        if (i > 0) {
            values = concat(values, buffer, i);
        }
    }

    public void accumulate(float... samples) {
        values = concat(values, samples, samples.length);
    }

    void clear() {
        values = new float[0];
    }

    public float[] getValues() {
        return values;
    }

    private static float[] concat(float[] a1, float[] a2, int a2Length) {
        float[] b = new float[a1.length + a2Length];
        System.arraycopy(a1, 0, b, 0, a1.length);
        System.arraycopy(a2, 0, b, a1.length, a2Length);
        return b;
    }
}
