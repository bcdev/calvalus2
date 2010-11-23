package com.bc.calvalus.b3;

/**
 * An aggregator that computes a maximum-likelihood average.
 */
public class AggregatorAverageML extends AggregatorAverage {

    public AggregatorAverageML(VariableContext ctx, String varName) {
        super(ctx, varName);
    }

    @Override
    public String getName() {
        return "AVG_ML";
    }

    @Override
    public void aggregateSpatial(Vector observationVector, WritableVector spatialVector) {
        final float value = (float) Math.log(observationVector.get(varIndex));
        spatialVector.set(0, spatialVector.get(0) + value);
        spatialVector.set(1, spatialVector.get(1) + value * value);
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        float sumX = temporalVector.get(0);
        float sumXX = temporalVector.get(1);
        float sumW = temporalVector.get(2);
        float avLogs = sumX / sumW;
        float vrLogs = sumXX / sumW - avLogs * avLogs;
        float xMean = (float) Math.exp(avLogs + 0.5 * vrLogs);
        outputVector.set(0, xMean);
    }
}
