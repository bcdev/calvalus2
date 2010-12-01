package com.bc.calvalus.b3;

import java.util.Arrays;

import static com.bc.calvalus.b3.AggregatorAverage.getWeightFn;
import static java.lang.Math.exp;
import static java.lang.Math.sqrt;

/**
 * An aggregator that computes a maximum-likelihood average.
 */
public class AggregatorAverageML implements Aggregator {

    private final int varIndex;
    private final String[] spatialPropertyNames;
    private final String[] temporalPropertyNames;
    private final String[] outputPropertyNames;
    private final AggregatorAverage.WeightFn weightFn;

    public AggregatorAverageML(VariableContext ctx, String varName) {
        this(ctx, varName, 0.5);
    }

    public AggregatorAverageML(VariableContext ctx, String varName, double weightCoeff) {
        varIndex = ctx.getVariableIndex(varName);
        spatialPropertyNames = new String[]{varName + "_sum_x", varName + "_sum_xx"};
        temporalPropertyNames = new String[]{varName + "_sum_x", varName + "_sum_xx", varName + "_sum_w"};
        outputPropertyNames = new String[]{varName + "_mean", varName + "_sigma", varName + "_median", varName + "_mode"};
        weightFn = getWeightFn(weightCoeff);
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
        vector.set(0, 0.0f);
        vector.set(1, 0.0f);
    }

    @Override
    public void initTemporal(WritableVector vector) {
        vector.set(0, 0.0f);
        vector.set(1, 0.0f);
        vector.set(2, 0.0f);
    }

    @Override
    public void aggregateSpatial(Vector observationVector, WritableVector spatialVector) {
        final float value = (float) Math.log(observationVector.get(varIndex));
        spatialVector.set(0, spatialVector.get(0) + value);
        spatialVector.set(1, spatialVector.get(1) + value * value);
    }

    @Override
    public void completeSpatial(int numObs, WritableVector numSpatialObs) {
        final float w = weightFn.eval(numObs);
        numSpatialObs.set(0, numSpatialObs.get(0) / w);
        numSpatialObs.set(1, numSpatialObs.get(1) / w);
    }

    @Override
    public void aggregateTemporal(Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        temporalVector.set(0, temporalVector.get(0) + spatialVector.get(0));
        temporalVector.set(1, temporalVector.get(1) + spatialVector.get(1));
        temporalVector.set(2, temporalVector.get(2) + weightFn.eval(numSpatialObs));
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        final float sumX = temporalVector.get(0);
        final float sumXX = temporalVector.get(1);
        final float sumW = temporalVector.get(2);
        final float avLogs = sumX / sumW;
        final float vrLogs = sumXX / sumW - avLogs * avLogs;
        final float mean = (float) exp(avLogs + 0.5 * vrLogs);
        final float sigma = (float) (mean * sqrt(exp(vrLogs) - 1.0));
        final float median = (float) exp(avLogs);
        final float mode = (float) exp(avLogs - vrLogs);
        outputVector.set(0, mean);
        outputVector.set(1, sigma);
        outputVector.set(2, median);
        outputVector.set(3, mode);
    }

    @Override
    public String toString() {
        return "AggregatorAverageML{" +
                "varIndex=" + varIndex +
                ", outputPropertyNames=" + (outputPropertyNames == null ? null : Arrays.toString(outputPropertyNames)) +
                ", weightFn=" + weightFn +
                '}';
    }
}
