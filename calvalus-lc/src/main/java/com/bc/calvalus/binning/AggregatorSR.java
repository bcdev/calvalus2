/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.binning;

import com.bc.ceres.binding.PropertyDescriptor;
import com.bc.ceres.binding.PropertySet;

/**
 * An aggregator to compute surface reflectance and errors in multiple wavelengths
 * from data containing multiple days.
 *
 * @author MarcoZ
 */
public class AggregatorSR extends AbstractAggregator {

    private static final String SDR_FLAGS_NAME = "status";
    private static final int STATUS_LAND = 1;
    private static final int STATUS_WATER = 2;
    private static final int STATUS_SNOW = 3;
    private static final int STATUS_CLOUD = 4;
    private static final int STATUS_CLOUD_SHADOW = 5;
    private static final String[] COUNTER_NAMES = {"land", "water", "snow", "cloud", "cloud_shadow"};

    private final int[] varIndexes;
    private final int numSdrBands;

    public AggregatorSR(VariableContext varCtx, int numSdrBands, Float fillValue) {
        super(Descriptor.NAME, createSpatialFeatureNames(numSdrBands), fillValue);
        this.numSdrBands = numSdrBands;
        if (varCtx == null) {
            throw new NullPointerException("varCtx");
        }
        varIndexes = createVariableIndexes(varCtx, numSdrBands);
    }


    private static int[] createVariableIndexes(VariableContext varCtx, int numBands) {
        int[] varIndexes = new int[1 + numBands + numBands];
        int j = 0;
        varIndexes[j++] = getVariableIndex(varCtx, SDR_FLAGS_NAME);
        for (int i = 0; i < numBands; i++) {
            varIndexes[j++] = getVariableIndex(varCtx, "sdr_" + (i + 1));
        }
        for (int i = 0; i < numBands; i++) {
            varIndexes[j++] = getVariableIndex(varCtx, "sdr_error_" + (i + 1));
        }
        return varIndexes;
    }

    private static int getVariableIndex(VariableContext varCtx, String varName) {
        int varIndex = varCtx.getVariableIndex(varName);
        if (varIndex < 0) {
            throw new IllegalArgumentException("varIndex < 0");
        }
        return varIndex;
    }

    private static String[] createSpatialFeatureNames(int numBands) {
        String[] featureNames = new String[COUNTER_NAMES.length + numBands + numBands];
        int j = 0;
        for (String counter : COUNTER_NAMES) {
            featureNames[j++] = counter + "_count";
        }
        for (int i = 0; i < numBands; i++) {
            featureNames[j++] = "sdr_" + (i + 1) + "_sum_x";
        }
        for (int i = 0; i < numBands; i++) {
            featureNames[j++] = "sdr_error_" + (i + 1) + "_sum_xx";
        }
        return featureNames;
    }


    @Override
    public void initSpatial(BinContext ctx, WritableVector vector) {
        int numBands = getSpatialFeatureNames().length;
        for (int i = 0; i < numBands; i++) {
            vector.set(i, 0.0f);
        }
    }

    @Override
    public void aggregateSpatial(BinContext ctx, Vector observationVector, WritableVector spatialVector) {
        final int status = (int) observationVector.get(varIndexes[0]);
        if (status == STATUS_LAND) {
            int landCount = (int) spatialVector.get(0);
            int snowCount = (int) spatialVector.get(2);
            int sdrOffset = COUNTER_NAMES.length;
            if (landCount == 0 && snowCount > 0) {
                for (int i = 0; i < numSdrBands + numSdrBands; i++) {
                    spatialVector.set(sdrOffset + i, 0.0f);
                }
            }
            for (int i = 0; i < numSdrBands; i++) {
                float value = observationVector.get(varIndexes[1 + i]);
                spatialVector.set(sdrOffset + i, spatialVector.get(sdrOffset + i) + value);
            }
            sdrOffset += numSdrBands;
            for (int i = 0; i < numSdrBands; i++) {
                float value = observationVector.get(varIndexes[1 + numSdrBands + i]);
                spatialVector.set(sdrOffset + i, spatialVector.get(sdrOffset + i) + (value * value));
            }
            spatialVector.set(0, landCount + 1);
        } else if (status == STATUS_WATER) {
            spatialVector.set(1, spatialVector.get(1) + 1);
        } else if (status == STATUS_SNOW) {
            int landCount = (int) spatialVector.get(0);
            if (landCount == 0) {
                int sdrOffset = COUNTER_NAMES.length;
                for (int i = 0; i < numSdrBands; i++) {
                    float value = observationVector.get(varIndexes[1 + i]);
                    spatialVector.set(sdrOffset + i, spatialVector.get(sdrOffset + i) + value);
                }
                sdrOffset += numSdrBands;
                for (int i = 0; i < numSdrBands; i++) {
                    float value = observationVector.get(varIndexes[1 + numSdrBands + i]);
                    spatialVector.set(sdrOffset + i, spatialVector.get(sdrOffset + i) + (value * value));
                }
            }
            spatialVector.set(2, spatialVector.get(2) + 1);
        } else if (status == STATUS_CLOUD) {
            spatialVector.set(3, spatialVector.get(3) + 1);
        } else if (status == STATUS_CLOUD_SHADOW) {
            spatialVector.set(4, spatialVector.get(4) + 1);
        }
    }

    @Override
    public void completeSpatial(BinContext ctx, int numSpatialObs, WritableVector spatialVector) {
    }

    @Override
    public void initTemporal(BinContext ctx, WritableVector vector) {
        int numBands = getTemporalFeatureNames().length;
        for (int i = 0; i < numBands; i++) {
            vector.set(i, 0.0f);
        }
    }

    @Override
    public void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        for (int i = 0; i < COUNTER_NAMES.length; i++) {
            temporalVector.set(i, temporalVector.get(i) + spatialVector.get(i));
        }
    }

    @Override
    public void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector) {
        //TODO
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        //TODO
    }

    public static class Descriptor implements AggregatorDescriptor {

        public static final String NAME = "LC_SR";

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public PropertyDescriptor[] getParameterDescriptors() {

            return new PropertyDescriptor[]{
                    new PropertyDescriptor("numSdrBands", Integer.class),
//                    new PropertyDescriptor("weightCoeff", Double.class),
                    new PropertyDescriptor("fillValue", Float.class),
            };
        }

        @Override
        public Aggregator createAggregator(VariableContext varCtx, PropertySet propertySet) {
            return new AggregatorSR(varCtx,
                                    propertySet.<Integer>getValue("numSdrBands"),
//                                         propertySet.<Double>getValue("weightCoeff"),
                                    propertySet.<Float>getValue("fillValue")
            );
        }
    }
}
