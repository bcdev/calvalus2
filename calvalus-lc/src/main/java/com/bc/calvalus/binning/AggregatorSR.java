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

import java.util.ArrayList;
import java.util.List;

/**
 * An aggregator to compute surface reflectance and errors in multiple wavelengths
 * from data containing multiple days.
 *
 * @author MarcoZ
 */
public class AggregatorSR extends AbstractAggregator {

    private static final int STATUS_LAND = 1;
    private static final int STATUS_WATER = 2;
    private static final int STATUS_SNOW = 3;
    private static final int STATUS_CLOUD = 4;
    private static final int STATUS_CLOUD_SHADOW = 5;
    private static final int STATUS_INVALID = 6;
    private static final String[] COUNTER_NAMES = {"land", "water", "snow", "cloud", "cloud_shadow"};

    private static final int SBIN_SDR_OFFSET = COUNTER_NAMES.length;

    private static final int TBIN_STATUS_INDEX = 0;
    private static final int TBIN_COUNT_OFFSET = 1;
    private static final int TBIN_W_SUM_INDEX = COUNTER_NAMES.length + 1;
    private static final int TBIN_SDR_OFFSET = COUNTER_NAMES.length + 2;

    private static final int OBIN_SDR_OFFSET = COUNTER_NAMES.length + 1;

    private static final String TEMPORAL_DATA = "temporalData";

    private final int[] varIndexes;
    private final int numSdrBands;

    public AggregatorSR(VariableContext varCtx, int numSdrBands, Float fillValue) {
        super(Descriptor.NAME,
              createSpatialFeatureNames(numSdrBands),
              createTemporalFeatureNames(numSdrBands),
              createOutputFeatureNames(numSdrBands),
              fillValue);
        this.numSdrBands = numSdrBands;
        if (varCtx == null) {
            throw new NullPointerException("varCtx");
        }
        varIndexes = createVariableIndexes(varCtx, numSdrBands);
    }


    private static int[] createVariableIndexes(VariableContext varCtx, int numBands) {
        int[] varIndexes = new int[3 + numBands + 1 + numBands];
        int j = 0;
        varIndexes[j++] = getVariableIndex(varCtx, "status");
        for (int i = 0; i < numBands; i++) {
            varIndexes[j++] = getVariableIndex(varCtx, "sdr_" + (i + 1));
        }
        varIndexes[j++] = getVariableIndex(varCtx, "ndvi");
        for (int i = 0; i < numBands; i++) {
            varIndexes[j++] = getVariableIndex(varCtx, "sdr_error_" + (i + 1));
        }
        return varIndexes;
    }

    private static int getVariableIndex(VariableContext varCtx, String varName) {
        int varIndex = varCtx.getVariableIndex(varName);
        if (varIndex < 0) {
            throw new IllegalArgumentException(String.format("varIndex < 0 for varName='%s'", varName));
        }
        return varIndex;
    }

    private static String[] createSpatialFeatureNames(int numBands) {
        String[] featureNames = new String[COUNTER_NAMES.length + numBands * 2 + 1];
        int j = 0;
        for (String counter : COUNTER_NAMES) {
            featureNames[j++] = counter + "_count";
        }
        for (int i = 0; i < numBands; i++) {
            featureNames[j++] = "sdr_" + (i + 1) + "_sum_x";
        }
        featureNames[j++] = "ndvi_sum_x";
        for (int i = 0; i < numBands; i++) {
            featureNames[j++] = "sdr_error_" + (i + 1) + "_sum_xx";
        }
        return featureNames;
    }

    private static String[] createTemporalFeatureNames(int numBands) {
        String[] featureNames = new String[3 + COUNTER_NAMES.length + (numBands * 2)];
        int j = 0;
        featureNames[j++] = "status";
        for (String counter : COUNTER_NAMES) {
            featureNames[j++] = counter + "_count";
        }
        featureNames[j++] = "w_sum";
        for (int i = 0; i < numBands; i++) {
            featureNames[j++] = "sdr_" + (i + 1) + "_sum_x";
        }
        featureNames[j++] = "ndvi_sum_x";
        for (int i = 0; i < numBands; i++) {
            featureNames[j++] = "sdr_error_" + (i + 1) + "_sum_xx";
        }
        return featureNames;
    }

    private static String[] createOutputFeatureNames(int numBands) {
        String[] featureNames = new String[2 + COUNTER_NAMES.length + (numBands * 2)];
        int j = 0;
        featureNames[j++] = "status";
        for (String counter : COUNTER_NAMES) {
            featureNames[j++] = counter + "_count";
        }
        for (int i = 0; i < numBands; i++) {
            featureNames[j++] = "sr_" + (i + 1) + "_mean";
        }
        featureNames[j++] = "ndvi_mean";
        for (int i = 0; i < numBands; i++) {
            featureNames[j++] = "sr_" + (i + 1) + "_sigma";
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
            // If we haven't seen LAND so far, but we had SNOW, clear SDRs
            if (landCount == 0 && snowCount > 0) {
                for (int i = 0; i < numSdrBands + numSdrBands + 1; i++) {
                    spatialVector.set(SBIN_SDR_OFFSET + i, 0.0f);
                }
            }
            // SSince we have seen LAND now, accumulate LAND SDRs
            addSpatialSdrs(observationVector, spatialVector);
            // Count LAND
            spatialVector.set(0, landCount + 1);
        } else if (status == STATUS_WATER) {
            // Count WATER
            spatialVector.set(1, spatialVector.get(1) + 1);
        } else if (status == STATUS_SNOW) {
            int landCount = (int) spatialVector.get(0);
            // If we haven't seen LAND so far, accumulate SNOW SDRs
            if (landCount == 0) {
                addSpatialSdrs(observationVector, spatialVector);
            }
            // Count SNOW
            spatialVector.set(2, spatialVector.get(2) + 1);
        } else if (status == STATUS_CLOUD) {
            // Count CLOUD
            spatialVector.set(3, spatialVector.get(3) + 1);
        } else if (status == STATUS_CLOUD_SHADOW) {
            // Count CLOUD_SHADOW
            spatialVector.set(4, spatialVector.get(4) + 1);
        }
    }

    private void addSpatialSdrs(Vector observationVector, WritableVector spatialVector) {
        final int sdrObservationOffset = 1; // status
        int sdrOffset = SBIN_SDR_OFFSET;
        for (int i = 0; i < numSdrBands + 1; i++) { // sdr + ndvi
            float sdrMeasurement = observationVector.get(varIndexes[sdrObservationOffset + i]);
            spatialVector.set(sdrOffset + i, spatialVector.get(sdrOffset + i) + sdrMeasurement);
        }
        sdrOffset += numSdrBands + 1;
        for (int i = 0; i < numSdrBands; i++) {  // sdr_error
            float sdrErrorMeasurement = observationVector.get(varIndexes[sdrObservationOffset + numSdrBands + 1 + i]);
            spatialVector.set(sdrOffset + i, spatialVector.get(sdrOffset + i) + (sdrErrorMeasurement * sdrErrorMeasurement));
        }
    }

    @Override
    public void completeSpatial(BinContext ctx, int numSpatialObs, WritableVector spatialVector) {
        int sum = 0;
        for (int i = 0; i < COUNTER_NAMES.length; i++) {
            sum += spatialVector.get(i);
        }
        float threshold = sum * 0.4f;

        int landCount = (int) spatialVector.get(0);
        int snowCount = (int) spatialVector.get(2);
        if (landCount < threshold && snowCount < threshold) {
            // clear pixel
            for (int i = 0; i < numSdrBands + numSdrBands + 1; i++) {
                spatialVector.set(SBIN_SDR_OFFSET + i, 0.0f);
            }
            for (int i = 0; i < COUNTER_NAMES.length; i++) {
                spatialVector.set(i, 0);
            }
        }

    }

    @Override
    public void initTemporal(BinContext ctx, WritableVector vector) {
        int numBands = getTemporalFeatureNames().length;
        for (int i = 0; i < numBands; i++) {
            vector.set(i, 0.0f);
        }
        ctx.put(TEMPORAL_DATA, new TemporalData());
    }

    @Override
    public void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        TemporalData temporalData = ctx.get(TEMPORAL_DATA);
        temporalData.addSpatial(spatialVector);
    }

    private void aggregateTemporalImpl(Vector spatialVector, WritableVector temporalVector, int[] temporalCounters, boolean isCloud) {
        for (int i = 0; i < COUNTER_NAMES.length; i++) {
            int spatialCount = (int) spatialVector.get(i);
            if (spatialCount > 0) {
                if (isCloud && i == 0) {
                    // if when have re-labeled a land pixel to a cloud pixel
                    // count the land counting values as cloud
                    temporalCounters[3] += spatialCount;
                    temporalCounters[COUNTER_NAMES.length + 3] += 1;
                } else {
                    temporalCounters[i] += spatialCount;
                    temporalCounters[COUNTER_NAMES.length + i] += 1;
                }
            }
        }
        if (!isCloud) {

            int landAreaCount = (int) spatialVector.get(0);
            int snowAreaCount = (int) spatialVector.get(2);
            int statusIndex = temporalCounters.length - 1;
            int status = temporalCounters[statusIndex];

            if (landAreaCount > 0) {
                if (status == STATUS_SNOW) {
                    //delete previous snow sums
                    for (int i = 0; i < numSdrBands + numSdrBands + 1; i++) {
                        temporalVector.set(TBIN_SDR_OFFSET + i, 0.0f);
                    }
                    temporalVector.set(TBIN_W_SUM_INDEX, 0.0f);
                }
                if (status != STATUS_LAND) {
                    temporalCounters[statusIndex] = STATUS_LAND;
                }
                addTemporalSdrs(spatialVector, temporalVector, landAreaCount);
            } else if (snowAreaCount > 0 && status != STATUS_LAND) {
                temporalCounters[statusIndex] = STATUS_SNOW;
                addTemporalSdrs(spatialVector, temporalVector, snowAreaCount);
            }
        }
    }

    private void addTemporalSdrs(Vector spatialVector, WritableVector temporalVector, int counts) {
        float w = (float) (1f / Math.sqrt(counts));
        for (int i = 0; i < numSdrBands + numSdrBands + 1; i++) { // sdr + ndvi + sdr_error
            float srdSum = spatialVector.get(SBIN_SDR_OFFSET + i) * w / counts;
            temporalVector.set(TBIN_SDR_OFFSET + i, temporalVector.get(TBIN_SDR_OFFSET + i) + srdSum);
        }
        temporalVector.set(TBIN_W_SUM_INDEX, temporalVector.get(TBIN_W_SUM_INDEX) + w);
    }

    @Override
    public void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector) {
        TemporalData temporalData = ctx.get(TEMPORAL_DATA);
        int numSpatialBins = temporalData.getSize();
        boolean[] isCloud = new boolean[numSpatialBins];
        if (numSpatialBins >= 2 && numSdrBands > 8) {
            float sdr8Sum = 0f;
            float sdr8SqrSum = 0f;
            int sdr8Count = 0;
            float[] sdr8s = new float[numSpatialBins];
            for (int i = 0; i < numSpatialBins; i++) {
                Vector spatialVector = temporalData.getVector(i);
                int landAreaCount = (int) spatialVector.get(0);
                if (landAreaCount > 0) {
                    float sdr = spatialVector.get(SBIN_SDR_OFFSET + 7) / landAreaCount;
                    sdr8s[i] = sdr;
                    sdr8Sum += sdr;
                    sdr8SqrSum += sdr * sdr;
                    sdr8Count++;
                }
            }

            if (sdr8Count >= 2) {
                float sdr8Mean = sdr8Sum / sdr8Count;
                float sdr8Sigma = (float) Math.sqrt(sdr8SqrSum / sdr8Count - sdr8Mean * sdr8Mean);
//                float cloudValue2 = sdr8Sigma / sdr8Mean;
//                if (cloudValue2 > 0.2f) {
                    float sdr8CloudDetector = sdr8Mean + sdr8Sigma;
                    for (int i = 0; i < numSpatialBins; i++) {
                        if (sdr8s[i] != 0 && sdr8s[i] > sdr8CloudDetector) {
                            // treat this as bin as cloud
                            isCloud[i] = true;
                        }
                    }
//                }
            }
        }
        int[] counters = new int[(COUNTER_NAMES.length * 2) + 1];
        for (int i = 0; i < numSpatialBins; i++) {
            Vector spatialVector = temporalData.getVector(i);
            aggregateTemporalImpl(spatialVector, temporalVector, counters, isCloud[i]);
        }
        int status = calculateStatus(counters[0], counters[1], counters[2], counters[3], counters[4]);
        temporalVector.set(TBIN_STATUS_INDEX, status);

        for (int i = 0; i < COUNTER_NAMES.length; i++) {
            temporalVector.set(TBIN_COUNT_OFFSET + i, counters[i]);
        }
    }

    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        for (int i = 0; i < COUNTER_NAMES.length + 1; i++) { // status and counters
            outputVector.set(i, temporalVector.get(i));
        }

        float wSum = temporalVector.get(TBIN_W_SUM_INDEX);
        for (int i = 0; i < numSdrBands + numSdrBands + 1; i++) {  // sdr + ndvi + sdr_error
            float sdrSum = temporalVector.get(TBIN_SDR_OFFSET + i);
            outputVector.set(OBIN_SDR_OFFSET + i, sdrSum / wSum);
        }
    }

    static int calculateStatus(float land, float water, float snow, float cloud, float cloudShadow) {
        if (land > 0) {
            return STATUS_LAND;
        } else if (water > 0 || snow > 0) {
            if (water > snow) {
                return STATUS_WATER;
            } else {
                return STATUS_SNOW;
            }
        } else if (cloud > 0 || cloudShadow > 0) {
            if (cloud > cloudShadow) {
                return STATUS_CLOUD;
            } else {
                return STATUS_CLOUD_SHADOW;
            }
        } else {
            return STATUS_INVALID;
        }
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
                    new PropertyDescriptor("fillValue", Float.class),
            };
        }

        @Override
        public Aggregator createAggregator(VariableContext varCtx, PropertySet propertySet) {
            return new AggregatorSR(varCtx,
                                    15, //propertySet.<Integer>getValue("numSdrBands"),
                                    propertySet.<Float>getValue("fillValue")
            );
        }
    }

    private class TemporalData {
        final List<Vector> vectorList;

        public TemporalData() {
            this.vectorList = new ArrayList<Vector>();
        }

        void addSpatial(Vector spatialVector) {
            vectorList.add(spatialVector);
        }

        int getSize() {
            return vectorList.size();
        }

        Vector getVector(int index) {
            return vectorList.get(index);
        }
    }
}
