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
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.annotations.Parameter;

import java.text.ParseException;
import java.util.Arrays;

/**
 * An aggregator that selects the youngest spectrum.
 */
public final class AggregatorYoungest extends AbstractAggregator {

    private final int[] varIndices;
    private final int numVariables;
    private final float referenceMjd;

    public AggregatorYoungest(VariableContext varCtx, String mjdVarName, float referenceMjd, String... varNames) {
        super(Descriptor.NAME,
              createOutputFeatureNames(mjdVarName, varNames),
              createOutputFeatureNames(mjdVarName, varNames),
              createOutputFeatureNames(mjdVarName, varNames));
        if (varCtx == null) {
            throw new NullPointerException("varCtx");
        }
        if (varNames == null) {
            throw new NullPointerException("varNames");
        }
        if (varNames.length == 0) {
            throw new NullPointerException("varNames");
        }
        this.referenceMjd = referenceMjd;
        numVariables = varNames.length;
        varIndices = new int[varNames.length];
        for (int i = 0; i < varNames.length; i++) {
            int varIndex = varCtx.getVariableIndex(varNames[i]);
            if (varIndex < 0) {
                throw new IllegalArgumentException("varIndices[" + i + "] < 0");
            }
            varIndices[i] = varIndex;
        }
    }

    @Override
    public void initSpatial(BinContext ctx, WritableVector vector) {
        vector.set(0, Float.NEGATIVE_INFINITY);
        for (int i=1; i<= numVariables; ++i) {
            vector.set(1, Float.NaN);
        }
    }

    @Override
    public void aggregateSpatial(BinContext ctx, Observation observationVector, WritableVector spatialVector) {
        if (Float.isInfinite(spatialVector.get(0))) {
            spatialVector.set(0, (float) observationVector.getMJD());
            for (int i = 0; i < numVariables; i++) {
                spatialVector.set(i + 1, observationVector.get(varIndices[i]));
            }
        }
    }

    @Override
    public void completeSpatial(BinContext ctx, int numObs, WritableVector numSpatialObs) {
    }

    @Override
    public void initTemporal(BinContext ctx, WritableVector vector) {
        vector.set(0, Float.NEGATIVE_INFINITY);
        for (int i=1; i<= numVariables; ++i) {
            vector.set(1, Float.NaN);
        }
    }

    @Override
    public void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs,
                                  WritableVector temporalVector) {
        if (spatialVector.get(0) > temporalVector.get(0)) {
            for (int i = 0; i < numVariables+1; i++) {
                temporalVector.set(i, spatialVector.get(i));
            }
        }
    }

    @Override
    public void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector) {
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        if (Float.isInfinite(temporalVector.get(0))) {
            outputVector.set(0, Float.NaN);
        } else if (Float.isNaN(referenceMjd)) {
            outputVector.set(0, temporalVector.get(0));
        } else {
            outputVector.set(0, referenceMjd - temporalVector.get(0));
        }
        for (int i = 1; i < numVariables + 1; i++) {
            outputVector.set(i, temporalVector.get(i));
        }
    }

    @Override
    public String toString() {
        return "AggregatorYoungest{" +
               "varIndices=" + Arrays.toString(varIndices) +
               ", spatialFeatureNames=" + Arrays.toString(getSpatialFeatureNames()) +
               ", temporalFeatureNames=" + Arrays.toString(getTemporalFeatureNames()) +
               ", outputFeatureNames=" + Arrays.toString(getOutputFeatureNames()) +
               '}';
    }

    private static String[] createOutputFeatureNames(String mjdVarName, String[] varNames) {
        String[] featureNames = new String[varNames.length + 1];
        featureNames[0] = mjdVarName;
        System.arraycopy(varNames, 0, featureNames, 1, varNames.length);
        return featureNames;
    }

    public static class Config extends AggregatorConfig {

        @Parameter(label = "Source band names", notNull = true, description = "The bands to be included in the output.")
        String[] varNames;

        @Parameter(label = "referenceDate",
                   description = "Date to subtract the youngest observation's MJD from to get the age in days")
        String referenceDate;

        public Config() {
            this(null);
        }

        public Config(String referenceDate, String... varNames) {
            super(Descriptor.NAME);
            this.referenceDate = referenceDate;
            this.varNames = varNames;
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
            float referenceMjd;
            String mjdVarName;
            mjdVarName = "mjd";
                referenceMjd = Float.NaN;
            if (config.referenceDate != null) {
                try {
                    referenceMjd = (float) ProductData.UTC.parse(config.referenceDate, "yyyy-MM-dd'T'HH:mm:ss'Z'").getMJD();
                    mjdVarName = "age";
                } catch (ParseException e) {}
            }
            return new AggregatorYoungest(varCtx, mjdVarName, referenceMjd, config.varNames);
        }

        @Override
        public String[] getSourceVarNames(AggregatorConfig aggregatorConfig) {
            Config config = (Config) aggregatorConfig;
            return config.varNames;
        }

        @Override
        public String[] getTargetVarNames(AggregatorConfig aggregatorConfig) {
            Config config = (Config) aggregatorConfig;
            String[] varNames = new String[config.varNames.length + 1];
            if (config.referenceDate == null) {
                varNames[0] = "mjd";
            } else {
                varNames[0] = "age";
            }
            System.arraycopy(config.varNames, 0, varNames, 1, config.varNames.length);
            return varNames;
        }
    }

    public boolean requiresGrowableSpatialData() {
        return false;
    }
}
