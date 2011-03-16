package com.bc.calvalus.binning;

import java.util.Arrays;

/**
 * An aggregator that computes the minimum and maximum values.
 */
public class AggregatorMinMax implements Aggregator {
    private final int varIndex;
    private final String[] propertyNames;
    private final double fillValue;

    public AggregatorMinMax(VariableContext varCtx, String varName, Double fillValue) {
        if (varCtx == null) {
            throw new NullPointerException("varCtx");
        }
        if (varName == null) {
            throw new NullPointerException("varName");
        }
        this.varIndex = varCtx.getVariableIndex(varName);
        this.propertyNames = new String[]{varName + "_min", varName + "_max"};
        this.fillValue = fillValue != null ? fillValue : Double.NaN;
    }

    @Override
    public String getName() {
        return "MIN_MAX";
    }

    @Override
    public int getSpatialPropertyCount() {
        return 2;
    }

    @Override
    public String getSpatialPropertyName(int i) {
        return propertyNames[i];
    }

    @Override
    public int getTemporalPropertyCount() {
        return 2;
    }

    @Override
    public String getTemporalPropertyName(int i) {
        return propertyNames[i];
    }

    @Override
    public int getOutputPropertyCount() {
        return 2;
    }

    @Override
    public String getOutputPropertyName(int i) {
       return propertyNames[i];
    }

    @Override
    public double getOutputPropertyFillValue(int i) {
        return fillValue;
    }

    @Override
    public void initSpatial(BinContext ctx, WritableVector vector) {
        vector.set(0, Float.POSITIVE_INFINITY);
        vector.set(1, Float.NEGATIVE_INFINITY);
    }

    @Override
    public void initTemporal(BinContext ctx, WritableVector vector) {
        vector.set(0, Float.POSITIVE_INFINITY);
        vector.set(1, Float.NEGATIVE_INFINITY);
    }

    @Override
    public void aggregateSpatial(BinContext ctx, Vector observationVector, WritableVector spatialVector) {
        final float value = observationVector.get(varIndex);
        spatialVector.set(0, Math.min(spatialVector.get(0), value));
        spatialVector.set(1, Math.max(spatialVector.get(1), value));
    }

    @Override
    public void completeSpatial(BinContext ctx, int numObs, WritableVector numSpatialObs) {
    }

    @Override
    public void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        temporalVector.set(0, Math.min(temporalVector.get(0), spatialVector.get(0)));
        temporalVector.set(1, Math.max(temporalVector.get(1), spatialVector.get(1)));
    }

    @Override
    public void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector) {
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        outputVector.set(0, temporalVector.get(0));
        outputVector.set(1, temporalVector.get(1));
    }

    @Override
    public String toString() {
        return "AggregatorMinMax{" +
                "varIndex=" + varIndex +
                ", propertyNames=" + (propertyNames == null ? null : Arrays.toString(propertyNames)) +
                '}';
    }
}
