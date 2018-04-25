/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.beam;

import org.esa.snap.binning.AbstractAggregator;
import org.esa.snap.binning.Aggregator;
import org.esa.snap.binning.AggregatorConfig;
import org.esa.snap.binning.AggregatorDescriptor;
import org.esa.snap.binning.BinContext;
import org.esa.snap.binning.Observation;
import org.esa.snap.binning.VariableContext;
import org.esa.snap.binning.Vector;
import org.esa.snap.binning.WritableVector;
import org.esa.snap.binning.aggregators.AggregatorPercentile;
import org.esa.snap.binning.support.GrowableVector;
import org.esa.snap.core.gpf.annotations.Parameter;
import org.esa.snap.core.util.StringUtils;

import java.util.Arrays;

/**
 * An aggregator that computes the p-th percentile,
 * the value of a variable below which a certain percent (p) of observations fall,
 * and that selects a set of variables at that time point.
 *
 * @author Martin
 */
public class AggregatorOnPercentileSet extends AbstractAggregator {

    private final int varIndex;
    private final int percentage;
    private final int[] setIndexes;
    private final int numSetFeatures;
    private final String mlName;
    private final String slName;
    private final String icName;

    public AggregatorOnPercentileSet(VariableContext varCtx, String varName, String targetName, int percentage, String... setVarNames) {
        super(Descriptor.NAME,
              createOutputFeatureNames(varName, targetName, setVarNames),
              createOutputFeatureNames(varName, targetName, setVarNames),
              createOutputFeatureNames(varName, targetName, setVarNames));

        if (varCtx == null) {
            throw new NullPointerException("varCtx");
        }
        if (varName == null) {
            throw new NullPointerException("varName");
        }
        if (percentage < 0 || percentage > 100) {
            throw new IllegalArgumentException("percentage < 0 || percentage > 100");
        }
        this.varIndex = varCtx.getVariableIndex(varName);
        this.percentage = percentage;
        numSetFeatures = setVarNames.length;
        setIndexes = new int[setVarNames.length];
        for (int i = 0; i < setVarNames.length; i++) {
            int varIndex = varCtx.getVariableIndex(setVarNames[i]);
            if (varIndex < 0) {
                throw new IllegalArgumentException("setIndexes[" + i + "] < 0");
            }
            setIndexes[i] = varIndex;
        }
        this.mlName = "ml." + varName;
        this.icName = "ic." + varName;
        this.slName = "sl." + varName;
    }

    public boolean requiresGrowableSpatialData() {
        return false;
    }

    @Override
    public void initSpatial(BinContext ctx, WritableVector vector) {
        vector.set(0, 0.0f);
        vector.set(1, Float.NaN);
        for (int i = 0; i < numSetFeatures; i++) {
            vector.set(i+2, 0.0f);
        }
        ctx.put(icName, new int[1+numSetFeatures]);
    }

    @Override
    public void aggregateSpatial(BinContext ctx, Observation observationVector, WritableVector spatialVector) {
        int[] invalidCounts = null;
        float value = observationVector.get(varIndex);
        if (!Float.isNaN(value)) {
            spatialVector.set(0, spatialVector.get(0) + value);
        } else {
            // We count invalids rather than valid because it is more efficient.
            // (Key/value map operations are relatively slow, and it is more likely that we will receive valid measurements.)
            invalidCounts = (int[]) ctx.get(icName);
            invalidCounts[0]++;
        }
        spatialVector.set(1, (float) observationVector.getMJD());
        for (int i = 0; i < numSetFeatures; i++) {
            value = observationVector.get(setIndexes[i]);
            if (!Float.isNaN(value)) {
                spatialVector.set(2 + i, spatialVector.get(2 + i) + value);
            } else {
                if (invalidCounts == null) {
                    invalidCounts = (int[]) ctx.get(icName);
                }
                invalidCounts[1 + i]++;
            }
        }
    }

    @Override
    public void completeSpatial(BinContext ctx, int numSpatialObs, WritableVector spatialVector) {
        int[] invalidCounts = (int[]) ctx.get(icName);
        int effectiveCount = numSpatialObs - invalidCounts[0];
        if (effectiveCount > 0) {
            spatialVector.set(0, spatialVector.get(0) / effectiveCount);
            for (int i = 0; i < numSetFeatures; i++) {
                effectiveCount = numSpatialObs - invalidCounts[1+i];
                if (effectiveCount > 0) {
                    spatialVector.set(2+i, spatialVector.get(2+i) / effectiveCount);
                } else {
                    spatialVector.set(2+i, Float.NaN);
                }
            }
        } else {
            for (int i = 0; i < 2+numSetFeatures; i++) {
                spatialVector.set(i, Float.NaN);
            }
        }
    }

    @Override
    public void initTemporal(BinContext ctx, WritableVector vector) {
        vector.set(0, 0.0f);
        ctx.put(mlName, new GrowableVector(256));
        ctx.put(slName, new GrowableVector(64 * (1+numSetFeatures)));
    }

    @Override
    public void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        GrowableVector measurementsVec = ctx.get(mlName);
        float value = spatialVector.get(0);
        if (!Float.isNaN(value)) {
            measurementsVec.add(value);
            GrowableVector setVec = ctx.get(slName);
            setVec.add(spatialVector.get(1));
            for (int i = 0; i < numSetFeatures; ++i) {
                setVec.add(spatialVector.get(2+i));
            }
        }
    }

    @Override
    public void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector) {
        GrowableVector measurementsVec = ctx.get(mlName);
        float[] measurements = measurementsVec.getElements();
        if (measurements.length > 0) {
            Arrays.sort(measurements);
            float percentileValue = computeDiscretePercentile(percentage, measurements);
            measurements = measurementsVec.getElements();
            for (int j=0; j<measurements.length; ++j) {
                if (measurements[j] == percentileValue) {
                    temporalVector.set(0, percentileValue);
                    float[] setMeasurements = ((GrowableVector) ctx.get(slName)).getElements();
                    for (int i=0; i<1+numSetFeatures; ++i) {
                        temporalVector.set(1+i, setMeasurements[j*(1+numSetFeatures)+i]);
                    }
                    return;
                }
            }
        }
        for (int i=0; i<numSetFeatures+2; ++i) {
            temporalVector.set(i, Float.NaN);
        }
    }


    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        for (int i=0; i<numSetFeatures+2; ++i) {
            outputVector.set(i, temporalVector.get(i));
        }
    }

    @Override
    public String toString() {
        return "AggregatorPercentile{" +
               "varIndex=" + varIndex +
               ", percentage=" + percentage +
               ", spatialFeatureNames=" + Arrays.toString(getSpatialFeatureNames()) +
               ", temporalFeatureNames=" + Arrays.toString(getTemporalFeatureNames()) +
               ", outputFeatureNames=" + Arrays.toString(getOutputFeatureNames()) +
               '}';
    }

    /**
     * Computes the p-th percentile of an array of measurements,
     * determines the nearest measurement (like median).
     *
     * @param p            The percentage in percent ranging from 0 to 100.
     * @param measurements Sorted array of measurements.
     * @return The  p-th percentile.
     */
    public static float computeDiscretePercentile(int p, float[] measurements) {
        int N = measurements.length;
        float n = (p / 100.0F) * (N - 1) + 0.5f;
        int k = (int) Math.floor(n);
        if (k < 0) {
            return measurements[0];
        } else if (k >= N) {
            return measurements[N - 1];
        } else {
            return measurements[k];
        }
    }

    private static String[] createOutputFeatureNames(String varName, String targetName, String[] setVarNames) {
        if (StringUtils.isNullOrEmpty(targetName)) {
            throw new IllegalArgumentException("targetName must not be empty");
        }
        String[] featureNames = new String[setVarNames.length + 2];
        featureNames[0] = targetName;
        featureNames[1] = varName + "_mjd";
        System.arraycopy(setVarNames, 0, featureNames, 2, setVarNames.length);
        return featureNames;
    }

    public static class Config extends AggregatorConfig {

        @Parameter(label = "Source band name", notEmpty = true, notNull = true, description = "The source band used for aggregation.")
        String onPercentileVarName;
        @Parameter(label = "Target band name prefix (optional)", description = "The name prefix for the resulting bands. If empty, the source band name is used")
        String targetName;
        @Parameter(label = "Percentile", defaultValue = "90", interval = "[0,100]",
                   description = "The percentile to be created. Must be in the interval [0..100].")
        Integer percentage;
        @Parameter(label = "Source band names", notNull = true, description = "The source bands used for aggregation when maximum band reaches its maximum.")
        String[] setVarNames;

        public Config() {
            this(null, null, 90);
        }

        public Config(String targetName, String onPercentileVarName, int percentage, String... setVarNames) {
            super(Descriptor.NAME);
            this.targetName = targetName;
            this.onPercentileVarName = onPercentileVarName;
            this.percentage = percentage;
            this.setVarNames = setVarNames;
        }
    }


    private static int getEffectivePercentage(Integer percentage) {
        return (percentage != null ? percentage : 90);
    }

    public static class Descriptor implements AggregatorDescriptor {

        public static final String NAME = "ON_PERCENTILE_SET";

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public AggregatorConfig createConfig() {
            return new Config();
        }

        @Override
        public Aggregator createAggregator(VariableContext varCtx, AggregatorConfig aggregatorConfig) {
            Config config = (Config) aggregatorConfig;
            int percentage = getEffectivePercentage(config.percentage);
            String targetName = StringUtils.isNotNullAndNotEmpty(config.targetName)  ? config.targetName : (config.onPercentileVarName + "_p" + percentage);
            return new AggregatorOnPercentileSet(varCtx, config.onPercentileVarName, targetName, percentage, config.setVarNames);
        }

        @Override
        public String[] getSourceVarNames(AggregatorConfig aggregatorConfig) {
            Config config = (Config) aggregatorConfig;
            int varNameLength = 1;
            if (config.setVarNames != null) {
                varNameLength += config.setVarNames.length;
            }
            String[] varNames = new String[varNameLength];
            varNames[0] = config.onPercentileVarName;
            if (config.setVarNames != null) {
                System.arraycopy(config.setVarNames, 0, varNames, 1, config.setVarNames.length);
            }
            return varNames;
        }

        @Override
        public String[] getTargetVarNames(AggregatorConfig aggregatorConfig) {
            Config config = (Config) aggregatorConfig;
            int percentage = getEffectivePercentage(config.percentage);
            String targetName = StringUtils.isNotNullAndNotEmpty(config.targetName)  ? config.targetName : (config.onPercentileVarName + "_p" + percentage);
            String[] setVarNames = config.setVarNames != null ? config.setVarNames : new String[0];
            return createOutputFeatureNames(config.onPercentileVarName, targetName, setVarNames);
        }
    }
}
