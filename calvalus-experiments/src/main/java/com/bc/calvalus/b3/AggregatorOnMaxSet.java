package com.bc.calvalus.b3;

/**
 * An aggregator that sets an output if an input is maximal.
 */
public final class AggregatorOnMaxSet implements Aggregator {
    private final String[] propertyNames;
    private final int numProperties;
    private final int[] varIndexes;

    public AggregatorOnMaxSet(VariableContext ctx, String... varNames) {
        propertyNames = varNames.clone();
        propertyNames[0] = varNames[0] + "_max";
        numProperties = varNames.length;
        varIndexes = new int[varNames.length];
        for (int i = 0; i < varNames.length; i++) {
            int varIndex = ctx.getVariableIndex(varNames[i]);
            if (varIndex < 0) {
                throw new IllegalArgumentException("varIndex < 0");
            }
            varIndexes[i] = varIndex;
        }
    }

    @Override
    public String getName() {
        return "ON_MAX_SET";
    }

    @Override
    public int getSpatialPropertyCount() {
        return numProperties;
    }

    @Override
    public String getSpatialPropertyName(int i) {
        return propertyNames[i];
    }

    @Override
    public int getTemporalPropertyCount() {
        return numProperties;
    }

    @Override
    public String getTemporalPropertyName(int i) {
        return propertyNames[i];
    }

    @Override
    public int getOutputPropertyCount() {
        return numProperties; 
    }

    @Override
    public String getOutputPropertyName(int i) {
        return propertyNames[i];
    }

    @Override
    public void initSpatial(WritableVector vector) {
        vector.set(0, -Float.MAX_VALUE);
    }

    @Override
    public void initTemporal(WritableVector vector) {
        vector.set(0, -Float.MAX_VALUE);
    }

    @Override
    public void aggregateSpatial(Vector observationVector, WritableVector spatialVector) {
        final float value = observationVector.get(0);
        final float currentMax = spatialVector.get(0);
        if (value > currentMax) {
            spatialVector.set(0, value);
            for (int i = 1; i < numProperties; i++) {
                spatialVector.set(i, observationVector.get(varIndexes[i]));
            }
        }
    }

    @Override
    public void completeSpatial(int numObs, WritableVector numSpatialObs) {
    }

    @Override
    public void aggregateTemporal(Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        final float value = spatialVector.get(0);
        final float currentMax = temporalVector.get(0);
        if (value > currentMax) {
            temporalVector.set(0, value);
            for (int i = 1; i < numProperties; i++) {
                temporalVector.set(i, spatialVector.get(i));
            }
        }
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        for (int i = 1; i < numProperties; i++) {
            outputVector.set(i, temporalVector.get(i));
        }
    }
}
