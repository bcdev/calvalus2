package com.bc.calvalus.b3;

import static java.lang.Math.*;

/**
 * An aggregator that computes a maximum-likelihood average.
 */
public class AggregatorAverageML implements Aggregator {

    private final int varIndex;
    private final String[] spatialPropertyNames;
    private final String[] temporalPropertyNames;
    private final String[] outputPropertyNames;

    public AggregatorAverageML(VariableContext ctx, String varName) {
        varIndex = ctx.getVariableIndex(varName);
        spatialPropertyNames = new String[]{varName + "_sum_x", varName + "_sum_xx"};
        temporalPropertyNames = new String[]{varName + "_sum_x", varName + "_sum_xx", varName + "_sum_w"};
        outputPropertyNames = new String[]{varName + "_mean", varName + "_sigma", varName + "_median", varName + "_mode"};
    }

    @Override
    public String getName() {
        return "AVG_ML";
    }

    @Override
    public int getSpatialPropertyCount() {
        return 2;
    }

    @Override
    public String getSpatialPropertyName(int i) {
        return spatialPropertyNames[i];
    }

    @Override
    public int getTemporalPropertyCount() {
        return 3;
    }

    @Override
    public String getTemporalPropertyName(int i) {
        return temporalPropertyNames[i];
    }

    @Override
    public int getOutputPropertyCount() {
        return 4;
    }

    @Override
    public String getOutputPropertyName(int i) {
        return outputPropertyNames[i];
    }

    @Override
    public void initSpatial(WritableVector vector) {
    }

    @Override
    public void initTemporal(WritableVector vector) {
    }

    @Override
    public void aggregateSpatial(Vector observationVector, WritableVector spatialVector) {
        final float value = (float) Math.log(observationVector.get(varIndex));
        spatialVector.set(0, spatialVector.get(0) + value);
        spatialVector.set(1, spatialVector.get(1) + value * value);
    }

    @Override
    public void completeSpatial(WritableVector spatialVector, int numObs) {
        final float w = weight(numObs);
        spatialVector.set(0, spatialVector.get(0) / w);
        spatialVector.set(1, spatialVector.get(1) / w);
    }

    @Override
    public void aggregateTemporal(Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        temporalVector.set(0, temporalVector.get(0) + spatialVector.get(0));
        temporalVector.set(1, temporalVector.get(1) + spatialVector.get(1));
        temporalVector.set(2, temporalVector.get(2) + weight(numSpatialObs));
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        float sumX = temporalVector.get(0);
        float sumXX = temporalVector.get(1);
        float sumW = temporalVector.get(2);
        float avLogs = sumX / sumW;
        float vrLogs = sumXX / sumW - avLogs * avLogs;
        float mean = (float) exp(avLogs + 0.5 * vrLogs);
        float sigma = (float) (mean * sqrt(exp(vrLogs) - 1.0));
        float median = (float) exp(avLogs);
        float mode = (float) exp(avLogs - vrLogs);

        outputVector.set(0,mean);
        outputVector.set(1, sigma);
        outputVector.set(2, median);
        outputVector.set(3, mode);
    }

    private static float weight(int numObs) {
        return (float) sqrt(numObs);
    }
}
