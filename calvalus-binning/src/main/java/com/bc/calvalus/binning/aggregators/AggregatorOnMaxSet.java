package com.bc.calvalus.binning.aggregators;

import com.bc.calvalus.binning.AbstractAggregator;
import com.bc.calvalus.binning.BinContext;
import com.bc.calvalus.binning.VariableContext;
import com.bc.calvalus.binning.Vector;
import com.bc.calvalus.binning.WritableVector;

import java.util.Arrays;

/**
 * An aggregator that sets an output if an input is maximal.
 */
public final class AggregatorOnMaxSet extends AbstractAggregator {
    private final int[] varIndexes;
    private int numFeatures;

    public AggregatorOnMaxSet(VariableContext varCtx, String... varNames) {
        super("ON_MAX_SET", createFeatureNames(varNames), null);
        if (varCtx == null) {
            throw new NullPointerException("varCtx");
        }
        numFeatures = varNames.length;
        varIndexes = new int[varNames.length];
        for (int i = 0; i < varNames.length; i++) {
            int varIndex = varCtx.getVariableIndex(varNames[i]);
            if (varIndex < 0) {
                throw new IllegalArgumentException("varIndex < 0");
            }
            varIndexes[i] = varIndex;
        }
    }

    @Override
    public void initSpatial(BinContext ctx, WritableVector vector) {
        vector.set(0, Float.NEGATIVE_INFINITY);
    }

    @Override
    public void initTemporal(BinContext ctx, WritableVector vector) {
        vector.set(0, Float.NEGATIVE_INFINITY);
    }

    @Override
    public void aggregateSpatial(BinContext ctx, Vector observationVector, WritableVector spatialVector) {
        final float value = observationVector.get(varIndexes[0]);
        final float currentMax = spatialVector.get(0);
        if (value > currentMax) {
            spatialVector.set(0, value);
            for (int i = 1; i < numFeatures; i++) {
                spatialVector.set(i, observationVector.get(varIndexes[i]));
            }
        }
    }

    @Override
    public void completeSpatial(BinContext ctx, int numObs, WritableVector numSpatialObs) {
    }

    @Override
    public void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        final float value = spatialVector.get(0);
        final float currentMax = temporalVector.get(0);
        if (value > currentMax) {
            temporalVector.set(0, value);
            for (int i = 1; i < numFeatures; i++) {
                temporalVector.set(i, spatialVector.get(i));
            }
        }
    }

    @Override
    public void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector) {
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        for (int i = 0; i < numFeatures; i++) {
            outputVector.set(i, temporalVector.get(i));
        }
    }

    @Override
    public String toString() {
        return "AggregatorOnMaxSet{" +
                "varIndexes=" + Arrays.toString(varIndexes) +
                ", spatialFeatureNames=" + Arrays.toString(getSpatialFeatureNames()) +
                ", temporalFeatureNames=" + Arrays.toString(getTemporalFeatureNames()) +
                ", outputFeatureNames=" + Arrays.toString(getOutputFeatureNames()) +
                 '}';
    }

    private static String[] createFeatureNames(String[] varNames) {
        if (varNames == null) {
            throw new NullPointerException("varNames");
        }
        if (varNames.length == 0) {
            throw new IllegalArgumentException("varNames.length == 0");
        }
        String[] featureNames = varNames.clone();
        featureNames[0] = varNames[0] + "_max";
        return featureNames;
    }

}
