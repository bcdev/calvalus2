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
import org.esa.snap.binning.support.GrowableVector;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.annotations.Parameter;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * An aggregator that selects the youngest spectrum.
 */
public final class AggregatorYoungestClear extends AbstractAggregator {

    private final int[] varIndices;
    private final int flagsVarIndex;
    private final int percentileVarIndex;
    private final long strongCloudFilter;
    private final long weakCloudFilter;
    private final int lowerPercentileThreshold;
    private final int upperPercentileThreshold;
    private final int filterThreshold;
    private final float referenceMjd;

    private static boolean FIRST_DEBUG_EVENT = true;
    private static boolean DEBUG = false;

    public AggregatorYoungestClear(VariableContext varCtx,
                                   String flagsVarName, long strongCloudFilter, long weakCloudFilter, int filterThreshold,
                                   String percentileVarName, int lowerPercentileThreshold, int upperPercentileThreshold,
                                   String mjdVarName, float referenceMjd, String... varNames) {
        super(Descriptor.NAME,
              createIntermediateFeatureNames(mjdVarName, flagsVarName, percentileVarName, varNames),
              createIntermediateFeatureNames(mjdVarName, flagsVarName, percentileVarName, varNames),
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
        this.strongCloudFilter = strongCloudFilter;
        this.weakCloudFilter = weakCloudFilter;
        this.filterThreshold = filterThreshold;
        this.lowerPercentileThreshold = lowerPercentileThreshold;
        this.upperPercentileThreshold = upperPercentileThreshold;
        this.flagsVarIndex = varCtx.getVariableIndex(flagsVarName);
        this.percentileVarIndex = varCtx.getVariableIndex(percentileVarName);
        varIndices = new int[varNames.length];
        for (int i = 0; i < varNames.length; i++) {
            int varIndex = varCtx.getVariableIndex(varNames[i]);
            if (varIndex < 0) {
                throw new IllegalArgumentException(varNames[i] + " not found in input");
            }
            varIndices[i] = varIndex;
        }
    }

    @Override
    public void initSpatial(BinContext ctx, WritableVector vector) {
        for (int i = 0; i< varIndices.length; ++i) {
            vector.set(i, Float.NaN);
        }
        vector.set(varIndices.length, Float.NEGATIVE_INFINITY);
        vector.set(varIndices.length+1, Float.NaN);
        vector.set(varIndices.length+2, Float.NaN);
    }

    @Override
    public void aggregateSpatial(BinContext ctx, Observation observationVector, WritableVector spatialVector) {
        if (Float.isInfinite(spatialVector.get(varIndices.length))) {
            for (int i = 0; i < varIndices.length; i++) {
                spatialVector.set(i, observationVector.get(varIndices[i]));
            }
            spatialVector.set(varIndices.length, (float) observationVector.getMJD());
            spatialVector.set(varIndices.length+1, observationVector.get(flagsVarIndex));
            spatialVector.set(varIndices.length+2, observationVector.get(percentileVarIndex));
        }
    }

    @Override
    public void completeSpatial(BinContext ctx, int numObs, WritableVector numSpatialObs) {
    }

    @Override
    public void initTemporal(BinContext ctx, WritableVector vector) {
        ctx.put("obs_stack", new GrowableVector(64 * (varIndices.length+3)));
    }

    @Override
    public void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs,
                                  WritableVector temporalVector) {
        GrowableVector setVec = ctx.get("obs_stack");
        if (! Float.isInfinite(spatialVector.get(varIndices.length))) {
            for (int i = 0; i < varIndices.length + 3; ++i) {
                setVec.add(spatialVector.get(i));
            }
        }
    }

    static class ArrayIndexComparator implements Comparator<Integer>
    {
        private final Float[] array;
        public ArrayIndexComparator(Float[] array)
        {
            this.array = array;
        }
        @Override
        public int compare(Integer index1, Integer index2)
        {
            if (array[index1] == null) {
                return -1;
            }
            if (array[index2] == null) {
                return 1;
            }
            return array[index1].compareTo(array[index2]);
        }
    }

    @Override
    public void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector) {
        GrowableVector setVec = ctx.get("obs_stack");
        boolean[] marker = new boolean[numTemporalObs];
        Float[] mjds = new Float[numTemporalObs];
        long[] flags = new long[numTemporalObs];
        Float[] percVarValues = new Float[numTemporalObs];
        for (int j=0; j<numTemporalObs; ++j) {
            mjds[j] = setVec.get(j*(varIndices.length+3) + varIndices.length);
            flags[j] = (long) setVec.get(j*(varIndices.length+3) + varIndices.length+1);
            percVarValues[j] = setVec.get(j*(varIndices.length+3) + varIndices.length+2);
            if (DEBUG) {
                System.out.println(String.format("completeTemporal j=%d mjd=%.2f flag=%X perc=%.2f", j, mjds[j], flags[j], percVarValues[j]));
            }
        }
        // apply cloud mask, strong, else weak, else nothing such that at least 50% remains
        int numValidObs = 0;
        for (int j=0; j<numTemporalObs; ++j) {
            if ((flags[j] & strongCloudFilter) == 0) {
                marker[j] = true;
                ++numValidObs;
                if (DEBUG) {
                    System.out.println(String.format("strong %X accepts %d", strongCloudFilter, j));
                }
            } else {
                marker[j] = false;
                if (DEBUG) {
                    System.out.println(String.format("strong %X drops %d", strongCloudFilter, j));
                }
            }
        }
        // ... else weak
        if (numValidObs < numTemporalObs * filterThreshold / 100.0) {
            if (FIRST_DEBUG_EVENT && ! DEBUG) {
                FIRST_DEBUG_EVENT = false;
                System.out.println("falling back to weak cloud mask");
                System.out.println("numValidObs=" + numValidObs);
                System.out.println("numTemporalObs=" + numTemporalObs);
                System.out.println("filterThreshold=" + filterThreshold);
                System.out.println("numTemporalObs * filterThreshold / 100.0=" + (numTemporalObs * filterThreshold / 100.0));
                System.out.println("strongCloudFilter=" + strongCloudFilter);
                for (int j=0; j<numTemporalObs; ++j) {
                    System.out.println("mjds[" + j + "]=" + mjds[j]);
                    System.out.println("flags[" + j + "]=" + Long.toHexString(flags[j]));
                    System.out.println("flags[" + j + "] & strongCloudFilter=" + (flags[j] & strongCloudFilter));
                }
                System.out.println("numValidObs=" + numValidObs);
            }
            numValidObs = 0;
            for (int j=0; j<numTemporalObs; ++j) {
                if ((flags[j] & weakCloudFilter) == 0) {
                    marker[j] = true;
                    ++numValidObs;
                    if (DEBUG) {
                        System.out.println(String.format("weak %X accepts %d", weakCloudFilter, j));
                    }
                } else {
                    marker[j] = false;
                    if (DEBUG) {
                        System.out.println(String.format("weak %X drops %d", weakCloudFilter, j));
                    }
                }
            }
        }
        // ... else nothing
        if (numValidObs < numTemporalObs * filterThreshold / 100.0) {
            numValidObs = numTemporalObs;
            for (int j = 0; j < numTemporalObs; ++j) {
                marker[j] = true;
                if (DEBUG) {
                    System.out.println(String.format("no filter accepts %d", weakCloudFilter, j));
                }
            }
        }
        // sort unmasked contributions by percentile variable and chop off lower and upper end
        ArrayIndexComparator percVarComparator = new ArrayIndexComparator(percVarValues);
        Integer[] percVarIndex = new Integer[numValidObs];
        int k = 0;
        for (int j=0; j<numTemporalObs; ++j) {
            if (marker[j]) {
                percVarIndex[k++] = j;
            }
        }
        Arrays.sort(percVarIndex, percVarComparator);
        int start = (int) Math.floor(numValidObs * lowerPercentileThreshold / 100.0);
        int stop = (int) Math.ceil(numValidObs * upperPercentileThreshold / 100.0);
        if (DEBUG) {
            System.out.println(String.format("cutting by perc from %d (%d) to between %d and %d", numValidObs, numTemporalObs, start, stop));
        }
        if (stop <= start) {
            if (DEBUG) {
                System.out.println(String.format("nothing left between %d and %d", start, stop));
            }
            for (int i = 0; i < varIndices.length; ++i) {
                temporalVector.set(i, Float.NaN);
            }
            return;
        }
        // sort percentile subset by age
        Integer[] mjdIndex = new Integer[stop-start];
        System.arraycopy(percVarIndex, start, mjdIndex, 0, stop-start);
        ArrayIndexComparator mjdComparator = new ArrayIndexComparator(mjds);
        Arrays.sort(mjdIndex, mjdComparator);
        // select youngest
        int selected = mjdIndex[stop-start-1];
        if (DEBUG) {
            System.out.println(String.format("select youngest %d with mjd %f", selected, mjds[selected]));
        }
        for (int i = 0; i < varIndices.length+3; ++i) {
            temporalVector.set(i, setVec.get(selected*(varIndices.length+3)+i));
            if (DEBUG) {
                System.out.println(String.format("result %d is %9.3f", i, temporalVector.get(i)));
            }
        }
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        if (Float.isNaN(temporalVector.get(varIndices.length))) {
            outputVector.set(0, Float.NaN);
        } else if (Float.isNaN(referenceMjd)) {
            outputVector.set(0, temporalVector.get(varIndices.length));
        } else {
            outputVector.set(0, referenceMjd - temporalVector.get(varIndices.length));
        }
        for (int i = 0; i < varIndices.length; ++i) {
            outputVector.set(i+1, temporalVector.get(i));
        }
    }

    @Override
    public String toString() {
        return "AggregatorYoungestClear{" +
               "varIndices=" + Arrays.toString(varIndices) +
               ", spatialFeatureNames=" + Arrays.toString(getSpatialFeatureNames()) +
               ", temporalFeatureNames=" + Arrays.toString(getTemporalFeatureNames()) +
               ", outputFeatureNames=" + Arrays.toString(getOutputFeatureNames()) +
               '}';
    }

    private static String[] createIntermediateFeatureNames(String mjdVarName, String flagsVarName, String percentileVarName, String[] varNames) {
        String[] featureNames = new String[varNames.length + 3];
        System.arraycopy(varNames, 0, featureNames, 0, varNames.length);
        featureNames[varNames.length] = mjdVarName;
        featureNames[varNames.length+1] = flagsVarName;
        featureNames[varNames.length+2] = percentileVarName;
        return featureNames;
    }

    private static String[] createOutputFeatureNames(String mjdVarName, String[] varNames) {
        String[] featureNames = new String[varNames.length + 1];
        featureNames[0] = mjdVarName;
        System.arraycopy(varNames, 0, featureNames, 1, varNames.length);
        return featureNames;
    }

    public static class Config extends AggregatorConfig {

        @Parameter(label = "Pixel classification flags band name", defaultValue = "pixel_classif_flags")
        String flagsVarName;
        @Parameter(label = "Strong cloud filter mask", description = "Bit mask of strong cloud filter", defaultValue = "2066")
        long strongCloudFilter;
        @Parameter(label = "Weak cloud filter mask", description = "Bit mask of weak cloud filter", defaultValue = "8")
        long weakCloudFilter;
        @Parameter(label = "Filter threshold", description = "Percent of minimum remaining", defaultValue = "20")
        int filterThreshold;
        @Parameter(label = "Threshold band name", description = "Band to sort observations")
        String percentileVarName;
        @Parameter(label = "Lower threshold", description = "Percentile for cutting off dark pixels", defaultValue = "25")
        int lowerPercentileThreshold;
        @Parameter(label = "Upper threshold", description = "Percentile for cutting off bright pixels", defaultValue = "75")
        int upperPercentileThreshold;
        @Parameter(label = "referenceDate",
                   description = "Date to subtract the youngest observation's MJD from to get the age in days")
        String referenceDate;
        @Parameter(label = "Source band names", notNull = true, description = "The bands to be included in the output.")
        String[] varNames;

        public Config() {
            this("pixel_classif_flags", 2066, 8, 20,
                 null, 25, 75,
                 null);
        }

        public Config(String flagVarName, long strongCloudFilter, long weakCloudFilter, int filterThreshold,
                      String percentileVarName, int lowerPercentileThreshold, int upperPercentileThreshold,
                      String referenceDate, String... varNames) {
            super(Descriptor.NAME);
            this.flagsVarName = flagVarName;
            this.strongCloudFilter = strongCloudFilter;
            this.weakCloudFilter = weakCloudFilter;
            this.filterThreshold = filterThreshold;
            this.percentileVarName = percentileVarName;
            this.lowerPercentileThreshold = lowerPercentileThreshold;
            this.upperPercentileThreshold = upperPercentileThreshold;
            this.referenceDate = referenceDate;
            this.varNames = varNames;
        }
    }

    public static class Descriptor implements AggregatorDescriptor {

        public static final String NAME = "YOUNGEST_CLEAR";

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
            return new AggregatorYoungestClear(varCtx, config.flagsVarName, config.strongCloudFilter, config.weakCloudFilter, config.filterThreshold,
                                               config.percentileVarName, config.lowerPercentileThreshold, config.upperPercentileThreshold,
                                               mjdVarName, referenceMjd, config.varNames);
        }

        @Override
        public String[] getSourceVarNames(AggregatorConfig aggregatorConfig) { 
            Config config = (Config) aggregatorConfig;
            List<String> varNames = Arrays.asList(config.varNames);
            if (config.percentileVarName != null && !varNames.contains(config.percentileVarName) && config.flagsVarName != null && !varNames.contains(config.flagsVarName)) {
                String[] sourceVarNames = new String[config.varNames.length+2];
                sourceVarNames[config.varNames.length] = config.percentileVarName;
                sourceVarNames[config.varNames.length+1] = config.flagsVarName;
                System.arraycopy(config.varNames, 0, sourceVarNames, 1, config.varNames.length);
                return sourceVarNames;
            } else if (config.percentileVarName != null && !varNames.contains(config.percentileVarName)) {
                String[] sourceVarNames = new String[config.varNames.length+1];
                sourceVarNames[config.varNames.length] = config.percentileVarName;
                System.arraycopy(config.varNames, 0, sourceVarNames, 1, config.varNames.length);
                return sourceVarNames;
            } else if (config.flagsVarName != null && !varNames.contains(config.flagsVarName)) {
                String[] sourceVarNames = new String[config.varNames.length+1];
                sourceVarNames[config.varNames.length+1] = config.flagsVarName;
                System.arraycopy(config.varNames, 0, sourceVarNames, 1, config.varNames.length);
                return sourceVarNames;
            } else {
                return config.varNames;
            }
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
