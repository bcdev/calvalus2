package com.bc.calvalus.binning;

import java.util.Arrays;

/**
 * An aggregator that computes an average.
 */
public final class AggregatorAverage extends AbstractAggregator {
    private final int varIndex;
    private final WeightFn weightFn;

    public AggregatorAverage(VariableContext varCtx, String varName, Double weightCoeff, Float fillValue) {
        super("AVG",
              createFeatureNames(varName, "sum_x", "sum_xx"),
              createFeatureNames(varName, "sum_x", "sum_xx", "sum_w"),
              createFeatureNames(varName, "mean", "sigma"),
              fillValue);
        if (varCtx == null) {
            throw new NullPointerException("varCtx");
        }
        if (varName == null) {
            throw new NullPointerException("varName");
        }
        if (weightCoeff != null && weightCoeff < 0.0) {
            throw new IllegalArgumentException("weightCoeff < 0.0");
        }
        this.varIndex = varCtx.getVariableIndex(varName);
        this.weightFn = WeightFn.createPow(weightCoeff != null ? weightCoeff : 0.0);
    }

    @Override
    public void initSpatial(BinContext ctx, WritableVector vector) {
        vector.set(0, 0.0f);
        vector.set(1, 0.0f);
    }

    @Override
    public void initTemporal(BinContext ctx, WritableVector vector) {
        vector.set(0, 0.0f);
        vector.set(1, 0.0f);
        vector.set(2, 0.0f);
    }

    @Override
    public void aggregateSpatial(BinContext ctx, Vector observationVector, WritableVector spatialVector) {
        final float value = observationVector.get(varIndex);
        spatialVector.set(0, spatialVector.get(0) + value);
        spatialVector.set(1, spatialVector.get(1) + value * value);
    }

    @Override
    public void completeSpatial(BinContext ctx, int numSpatialObs, WritableVector spatialVector) {
        spatialVector.set(0, spatialVector.get(0) / numSpatialObs);
        spatialVector.set(1, spatialVector.get(1) / numSpatialObs);
    }

    @Override
    public void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        final float w = weightFn.eval(numSpatialObs);
        temporalVector.set(0, temporalVector.get(0) + spatialVector.get(0) * w);
        temporalVector.set(1, temporalVector.get(1) + spatialVector.get(1) * w);
        temporalVector.set(2, temporalVector.get(2) + w);
    }

    @Override
    public void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector) {
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        float sumX = temporalVector.get(0);
        float sumXX = temporalVector.get(1);
        float sumW = temporalVector.get(2);
        float mean = sumX / sumW;
        float sigma = (float) Math.sqrt(sumXX / sumW - mean * mean);
        outputVector.set(0, mean);
        outputVector.set(1, sigma);
    }

    @Override
    public String toString() {
        return "AggregatorAverage{" +
                ", varIndex=" + varIndex +
                ", weightFn=" + weightFn +
                ", spatialFeatureNames=" + Arrays.toString(getSpatialFeatureNames()) +
                ", temporalFeatureNames=" + Arrays.toString(getTemporalFeatureNames()) +
                ", outputFeatureNames=" + Arrays.toString(getOutputFeatureNames()) +
                '}';
    }


}
