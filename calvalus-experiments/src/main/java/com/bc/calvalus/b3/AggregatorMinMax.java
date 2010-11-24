package com.bc.calvalus.b3;

/**
 * An aggregator that computes the minimum and maximum values.
 */
public class AggregatorMinMax implements Aggregator {
    private final int varIndex;
    private String[] propertyNames;

    public AggregatorMinMax(VariableContext ctx, String varName) {
        varIndex = ctx.getVariableIndex(varName);
        propertyNames = new String[]{varName + "_min", varName + "_max"};
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
    public void initSpatial(WritableVector vector) {
        vector.set(0, +Float.MAX_VALUE);
        vector.set(1, -Float.MAX_VALUE);
    }

    @Override
    public void initTemporal(WritableVector vector) {
        vector.set(0, +Float.MAX_VALUE);
        vector.set(1, -Float.MAX_VALUE);
    }

    @Override
    public void aggregateSpatial(Vector observationVector, WritableVector spatialVector) {
        final float value = observationVector.get(varIndex);
        spatialVector.set(0, Math.min(spatialVector.get(0), value));
        spatialVector.set(1, Math.max(spatialVector.get(1), value));
    }

    @Override
    public void completeSpatial(int numObs, WritableVector numSpatialObs) {
    }

    @Override
    public void aggregateTemporal(Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        temporalVector.set(0, Math.min(temporalVector.get(0), spatialVector.get(0)));
        temporalVector.set(1, Math.max(temporalVector.get(1), spatialVector.get(1)));
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        outputVector.set(0, temporalVector.get(0));
        outputVector.set(1, temporalVector.get(1));
    }

}
