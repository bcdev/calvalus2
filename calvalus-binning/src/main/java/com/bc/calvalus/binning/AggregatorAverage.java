package com.bc.calvalus.binning;

import java.util.Arrays;

/**
 * An aggregator that computes an average.
 */
public final class AggregatorAverage implements Aggregator {
    private final int varIndex;
    private final String[] spatialPropertyNames;
    private final String[] temporalPropertyNames;
    private final String[] outputPropertyNames;
    private final WeightFn weightFn;

    public AggregatorAverage(VariableContext ctx, String varName) {
        this(ctx, varName, 1.0);
    }

    public AggregatorAverage(VariableContext varCtx, String varName, double weightCoeff) {
        if (varCtx == null) {
            throw new NullPointerException("varCtx");
        }
        if (varName == null) {
            throw new NullPointerException("varName");
        }
        if (weightCoeff <= 0.0) {
            throw new IllegalArgumentException("weightCoeff <= 0.0");
        }
        this.varIndex = varCtx.getVariableIndex(varName);
        this.spatialPropertyNames = new String[]{varName + "_sum_x", varName + "_sum_xx"};
        this.temporalPropertyNames = new String[]{varName + "_sum_x", varName + "_sum_xx", varName + "_sum_w"};
        this.outputPropertyNames = new String[]{varName + "_mean", varName + "_sigma"};
        this.weightFn = getWeightFn(weightCoeff);
    }

    @Override
    public String getName() {
        return "AVG";
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
        return 2;
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
        final float value = observationVector.get(varIndex);
        spatialVector.set(0, spatialVector.get(0) + value);
        spatialVector.set(1, spatialVector.get(1) + value * value);
    }

    @Override
    public void completeSpatial(int numSpatialObs, WritableVector spatialVector) {
        final float w = weightFn.eval(numSpatialObs);
        spatialVector.set(0, spatialVector.get(0) / w);
        spatialVector.set(1, spatialVector.get(1) / w);
    }

    @Override
    public void aggregateTemporal(Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        temporalVector.set(0, temporalVector.get(0) + spatialVector.get(0));
        temporalVector.set(1, temporalVector.get(1) + spatialVector.get(1));
        temporalVector.set(2, temporalVector.get(2) + weightFn.eval(numSpatialObs));
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
                "outputPropertyNames=" + (outputPropertyNames == null ? null : Arrays.toString(outputPropertyNames)) +
                ", varIndex=" + varIndex +
                ", weightFn=" + weightFn +
                '}';
    }

    public static WeightFn getWeightFn(double c) {
        return c == 0.5 ? new Sqrt() : c == 1.0 ? new Ident() : new Pow(c);
    }

    public static interface WeightFn {
        float eval(int numObs);
    }

    private final static class Ident implements WeightFn {
        @Override
        public float eval(int numObs) {
            return (float) numObs;
        }
    }

    private final static class Sqrt implements WeightFn {
        @Override
        public float eval(int numObs) {
            return (float) Math.sqrt(numObs);
        }
    }

    private final static class Pow implements WeightFn {
        private final double c;

        private Pow(double c) {
            this.c = c;
        }

        @Override
        public float eval(int numObs) {
            return (float) Math.pow(numObs, c);
        }
    }
}
