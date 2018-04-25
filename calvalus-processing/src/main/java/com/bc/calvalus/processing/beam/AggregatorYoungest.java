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
import org.esa.snap.binning.aggregators.AggregatorOnMaxSet;
import org.esa.snap.core.gpf.annotations.Parameter;
import org.esa.snap.core.util.StringUtils;

import java.util.Arrays;

/**
 * An aggregator that sets an output if an input is maximal.
 */
public final class AggregatorYoungest extends AbstractAggregator {

    private final int varIndex;

    public AggregatorYoungest(VariableContext varCtx, String varName, String targetVarName) {
        super(Descriptor.NAME, new String[] { varName, varName+"_mjd" }, new String[] { varName, varName+"_mjd" }, new String[] { targetVarName, varName+"_mjd" });

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
        vector.set(0, Float.NaN);
        vector.set(1, Float.NEGATIVE_INFINITY);
    }

    @Override
    public void initTemporal(BinContext ctx, WritableVector vector) {
        vector.set(0, Float.NaN);
        vector.set(1, Float.NEGATIVE_INFINITY);
    }

    @Override
    public void aggregateSpatial(BinContext ctx, Observation observationVector, WritableVector spatialVector) {
        final float value = observationVector.get(varIndex);
        if (! Float.isNaN(value)) {
            spatialVector.set(0, value);
            spatialVector.set(1, (float) observationVector.getMJD());
        }
    }

    @Override
    public void completeSpatial(BinContext ctx, int numObs, WritableVector numSpatialObs) {
    }

    @Override
    public void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs,
                                  WritableVector temporalVector) {
        final float value = spatialVector.get(0);
        final float mjd = spatialVector.get(1);
        final float currentMax = temporalVector.get(1);
        if (! Float.isNaN(value) && mjd > currentMax) {
            temporalVector.set(0, value);
            temporalVector.set(1, mjd);
        }
    }

    @Override
    public void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector) {
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        if (Float.isInfinite(temporalVector.get(1))) {
            outputVector.set(0, Float.NaN);
            outputVector.set(1, Float.NaN);
        } else {
            outputVector.set(0, temporalVector.get(0));
            outputVector.set(1, temporalVector.get(1));
        }
    }

    @Override
    public String toString() {
        return "AggregatorYoungest{" +
               "varIndex=" + varIndex +
               ", spatialFeatureNames=" + Arrays.toString(getSpatialFeatureNames()) +
               ", temporalFeatureNames=" + Arrays.toString(getTemporalFeatureNames()) +
               ", outputFeatureNames=" + Arrays.toString(getOutputFeatureNames()) +
               '}';
    }

    public boolean requiresGrowableSpatialData() {
        return false;
    }

    public static class Config extends AggregatorConfig {

        @Parameter(label = "Maximum band name", notEmpty = true, notNull = true,
                   description = "If this band reaches its maximum the values of the source bands are taken.")
        String varName;
        @Parameter(label = "Target band name prefix (optional)", description = "The name prefix for the resulting maximum bands. " +
                                                                    "If empty, the source band name is used")
        String targetName;

        public Config() {
            this(null, null);
        }

        public Config(String targetName, String varName) {
            super(Descriptor.NAME);
            this.targetName = targetName;
            this.varName = varName;
        }
    }

    public static class Descriptor implements AggregatorDescriptor {

        public static final String NAME = "YOUNGEST";

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
            String targetName = StringUtils.isNotNullAndNotEmpty(config.targetName)  ? config.targetName : config.varName;
            return new AggregatorOnMaxSet(varCtx, config.varName, targetName);
        }

        @Override
        public String[] getSourceVarNames(AggregatorConfig aggregatorConfig) {
            Config config = (Config) aggregatorConfig;
            return new String[] { config.varName };
        }

        @Override
        public String[] getTargetVarNames(AggregatorConfig aggregatorConfig) {
            Config config = (Config) aggregatorConfig;
            String targetName = StringUtils.isNotNullAndNotEmpty(config.targetName)  ? config.targetName : config.varName;
            return new String[] { targetName, config.varName + "_mjd" };
        }
    }
}
