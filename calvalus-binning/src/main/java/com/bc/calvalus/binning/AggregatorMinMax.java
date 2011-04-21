package com.bc.calvalus.binning;

import java.util.Arrays;

/**
 * An aggregator that computes the minimum and maximum values.
 */
public class AggregatorMinMax extends AbstractAggregator {
    private final int varIndex;

    public AggregatorMinMax(VariableContext varCtx, String varName, Float fillValue) {
        super("MIN_MAX", createFeatureNames(varName, "min", "max"), fillValue);

        if (varCtx == null) {
            throw new NullPointerException("varCtx");
        }
        if (varName == null) {
            throw new NullPointerException("varName");
        }
        this.varIndex = varCtx.getVariableIndex(varName);
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
                ", spatialFeatureNames=" + Arrays.toString(getSpatialFeatureNames()) +
                ", temporalFeatureNames=" + Arrays.toString(getTemporalFeatureNames()) +
                ", outputFeatureNames=" + Arrays.toString(getOutputFeatureNames()) +
                '}';
    }
}
