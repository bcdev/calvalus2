package com.bc.calvalus.processing.ma;

/**
 * Basically this class holds a number of statistics derived from an arry of numbers.
 * It is a {@link Number} so that its 'mean' value can be used as a numeric record value.
 *
 * @author MarcoZ
 * @author Norman
 */
public class AggregatedNumber extends Number {
    private final float sum;
    private final float mean;
    private final int numGoodPixels;
    private final int numTotalPixels;

    public AggregatedNumber(float sum, int numGoodPixels, int numTotalPixels) {
        this.sum = sum;
        this.mean = numGoodPixels > 0 ? sum / numGoodPixels : Float.NaN;
        this.numGoodPixels = numGoodPixels;
        this.numTotalPixels = numTotalPixels;
    }

    public float getSum() {
        return sum;
    }

    public float getMean() {
        return mean;
    }

    public int getNumGoodPixels() {
        return numGoodPixels;
    }

    public int getNumTotalPixels() {
        return numTotalPixels;
    }

    @Override
    public int intValue() {
        return Math.round(getMean());
    }

    @Override
    public long longValue() {
        return Math.round(getMean());
    }

    @Override
    public float floatValue() {
        return getMean();
    }

    @Override
    public double doubleValue() {
        return getMean();
    }

    @Override
    public String toString() {
        return String.valueOf(getMean());
    }
}
