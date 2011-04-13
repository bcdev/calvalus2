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
    private final double fillValue;

    public AggregatorAverage(VariableContext varCtx, String varName, Double weightCoeff, Double fillValue) {
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
        this.spatialPropertyNames = new String[]{varName + "_sum_x", varName + "_sum_xx"};
        this.temporalPropertyNames = new String[]{varName + "_sum_x", varName + "_sum_xx", varName + "_sum_w"};
        this.outputPropertyNames = new String[]{varName + "_mean", varName + "_sigma"};
        this.weightFn = getWeightFn(weightCoeff != null ? weightCoeff : 0.0);
        this.fillValue = fillValue != null ? fillValue : Double.NaN;
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
    public double getOutputPropertyFillValue(int i) {
        return fillValue;
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
                "outputPropertyNames=" + (outputPropertyNames == null ? null : Arrays.toString(outputPropertyNames)) +
                ", varIndex=" + varIndex +
                ", weightFn=" + weightFn +
                '}';
    }

    public static WeightFn getWeightFn(double c) {
        if (c == 0.0) {
            return new One();
        } else if (c == 0.5) {
            return new Sqrt();
        } else if (c == 1.0) {
            return new Ident();
        } else {
            return new Pow(c);
        }
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

    private final static class One implements WeightFn {
        @Override
        public float eval(int numObs) {
            return 1.0f;
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
