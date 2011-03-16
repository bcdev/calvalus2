package com.bc.calvalus.binning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * An aggregator that computes the minimum and maximum values.
 */
public class AggregatorPercentile implements Aggregator {
    private final int varIndex;
    private final String[] spatialPropertyNames;
    private final String[] temporalPropertyNames;
    private final int percentile;
    private final double fillValue;

    public AggregatorPercentile(VariableContext varCtx, String varName, int percentile, Double fillValue) {
        if (varCtx == null) {
            throw new NullPointerException("varCtx");
        }
        if (varName == null) {
            throw new NullPointerException("varName");
        }
        if (percentile < 0 || percentile > 100) {
            throw new IllegalArgumentException("percentile < 0 || percentile > 100");
        }
        this.varIndex = varCtx.getVariableIndex(varName);
        this.spatialPropertyNames = new String[]{varName + "_sum_x"};
        this.temporalPropertyNames = new String[]{varName + "_P" + percentile};
        this.percentile = percentile;
        this.fillValue = fillValue != null ? fillValue : Double.NaN;
    }

    @Override
    public String getName() {
        return "PERCENTILE";
    }

    @Override
    public int getSpatialPropertyCount() {
        return spatialPropertyNames.length;
    }

    @Override
    public String getSpatialPropertyName(int i) {
        return spatialPropertyNames[i];
    }

    @Override
    public int getTemporalPropertyCount() {
        return temporalPropertyNames.length;
    }

    @Override
    public String getTemporalPropertyName(int i) {
        return temporalPropertyNames[i];
    }

    @Override
    public int getOutputPropertyCount() {
        return temporalPropertyNames.length;
    }

    @Override
    public String getOutputPropertyName(int i) {
        return temporalPropertyNames[i];
    }

    @Override
    public double getOutputPropertyFillValue(int i) {
        return fillValue;
    }

    @Override
    public void initSpatial(BinContext ctx, WritableVector vector) {
        vector.set(0, 0.0f);
    }

    @Override
    public void aggregateSpatial(BinContext ctx, Vector observationVector, WritableVector spatialVector) {
        final float value = observationVector.get(varIndex);
        spatialVector.set(0, spatialVector.get(0) + value);
    }

    @Override
    public void completeSpatial(BinContext ctx, int numSpatialObs, WritableVector spatialVector) {
        spatialVector.set(0, spatialVector.get(0) / numSpatialObs);
    }

    @Override
    public void initTemporal(BinContext ctx, WritableVector vector) {
        vector.set(0, 0.0f);
        ctx.put("ml", new ArrayList<Float>());
    }

    @Override
    public void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        List<Float> measurementsList = ctx.get("ml");
        measurementsList.add(spatialVector.get(0));
    }

    @Override
    public void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector) {
        List<Float> measurementsList = ctx.get("ml");
        float[] measurements = new float[measurementsList.size()];
        for (int i = 0; i < measurements.length; i++) {
            measurements[i] = measurementsList.get(i);
        }
        Arrays.sort(measurements);
        temporalVector.set(0, computePercentile(percentile, measurements));
    }


    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        outputVector.set(0, temporalVector.get(0));
    }

    @Override
    public String toString() {
        return "AggregatorPercentile{" +
                "varIndex=" + varIndex +
                ", spatialPropertyNames=" + (spatialPropertyNames == null ? null : Arrays.toString(spatialPropertyNames)) +
                ", temporalPropertyNames=" + (temporalPropertyNames == null ? null : Arrays.toString(temporalPropertyNames)) +
                '}';
    }

    /**
     * Computes the p-th percentile of an array of measurements following
     * the "Engineering Statistics Handbook: Percentile". NIST.
     * http://www.itl.nist.gov/div898/handbook/prc/section2/prc252.htm.
     * Retrieved 2011-03-16.
     *
     * @param p            The percentage in percent ranging from 0 to 100.
     * @param measurements Sorted array of measurements.
     * @return The  p-th percentile.
     */
    public static float computePercentile(int p, float[] measurements) {
        int N = measurements.length;
        float n = (p / 100.0F) * (N + 1);
        int k = (int) Math.floor(n);
        float d = n - k;
        float yp;
        if (k == 0) {
            yp = measurements[0];
        } else if (k >= N) {
            yp = measurements[N - 1];
        } else {
            yp = measurements[k - 1] + d * (measurements[k] - measurements[k - 1]);
        }
        return yp;
    }
}
