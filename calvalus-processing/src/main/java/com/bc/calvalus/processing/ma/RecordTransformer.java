package com.bc.calvalus.processing.ma;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RecordTransformer {

    private final int maskAttributeIndex;
    private final double filteredMeanCoeff;
    private final RecordFilter[] recordFilters;

    public RecordTransformer(int maskAttributeIndex, double filteredMeanCoeff, RecordFilter... recordFilters) {
        this.maskAttributeIndex = maskAttributeIndex;
        this.filteredMeanCoeff = filteredMeanCoeff;
        this.recordFilters = recordFilters;
    }

    public List<Record> expand(Record record) {
        Object[] attributeValues = record.getAttributeValues();
        int numRecords = getCommonArrayValueLength(attributeValues);
        if (numRecords == -1) {
            return Arrays.asList(record);
        }
        List<Record> resultingRecords = new ArrayList<Record>(numRecords);
        int[] maskValues = getMaskValues(attributeValues);

        for (int recordIndex = 0; recordIndex < numRecords; recordIndex++) {
            if (isGoodPixel(maskValues, recordIndex)) {
                Object[] values = new Object[attributeValues.length];
                for (int valueIndex = 0; valueIndex < values.length; valueIndex++) {
                    Object attributeValue = attributeValues[valueIndex];
                    if (attributeValue != null && attributeValue.getClass().isArray()) {
                        values[valueIndex] = Array.get(attributeValue, recordIndex);
                    } else {
                        values[valueIndex] = attributeValue;
                    }
                }
                DefaultRecord resultingRecord = new DefaultRecord(record.getLocation(), record.getTime(), values);
                if (isRecordAccepted(resultingRecord)) {
                    resultingRecords.add(resultingRecord);
                }
            }
        }

        return resultingRecords;
    }

    private int[] getMaskValues(Object[] attributeValues) {
        int[] maskValues = null;
        if (maskAttributeIndex != -1) {
            maskValues = (int[]) attributeValues[maskAttributeIndex];
        }
        return maskValues;
    }

    public Record aggregate(Record record) {
        Object[] attributeValues = record.getAttributeValues();
        if (getCommonArrayValueLength(attributeValues) == -1) {
            return record;
        }
        int[] maskValues = getMaskValues(attributeValues);
        Object[] aggregatedValues = new Object[attributeValues.length];
        for (int valueIndex = 0; valueIndex < aggregatedValues.length; valueIndex++) {
            Object attributeValue = attributeValues[valueIndex];
            AggregatedNumber aggregatedNumber = null;
            if (attributeValue == maskValues) {
                aggregatedNumber = aggregate(maskValues, null);
            } else if (attributeValue instanceof float[]) {
                aggregatedNumber = aggregate((float[]) attributeValue, maskValues);
            } else if (attributeValue instanceof int[]) {
                aggregatedNumber = aggregate((int[]) attributeValue, maskValues);
            }
            if (aggregatedNumber != null) {
                aggregatedValues[valueIndex] = aggregatedNumber;
            } else {
                aggregatedValues[valueIndex] = attributeValue;
            }
        }
        DefaultRecord aggregatedRecord = new DefaultRecord(record.getLocation(), record.getTime(), aggregatedValues);
        if (!isRecordAccepted(aggregatedRecord)) {
            return null;
        }
        return aggregatedRecord;
    }

    protected AggregatedNumber aggregate(int[] values, int[] maskValues) {
        float[] floats = new float[values.length];
        for (int i = 0; i < floats.length; i++) {
            floats[i] = values[i];
        }
        return aggregate(floats, maskValues);
    }

    protected AggregatedNumber aggregate(float[] values, int[] maskValues) {

        // Step 1: Compute min, max, mean
        double sum = 0;
        double min = +Float.MAX_VALUE;
        double max = -Float.MAX_VALUE;
        int numGoodPixels = 0;
        int numTotalPixels = 0;
        for (int i = 0; i < values.length; i++) {
            final float value = values[i];
            if (!Float.isNaN(value)) {
                numTotalPixels++;
                if (isGoodPixel(maskValues, i)) {
                    numGoodPixels++;
                    sum += value;
                    min = Math.min(min, value);
                    max = Math.max(max, value);
                }
            }
        }
        final double mean = numGoodPixels > 0 ? sum / numGoodPixels : Float.NaN;

        // Step 2: Compute stdDev
        double sumSqrDev = 0;
        for (int i = 0; i < values.length; i++) {
            final float value = values[i];
            if (!Float.isNaN(value) && isGoodPixel(maskValues, i)) {
                sumSqrDev += (mean - value) * (mean - value);
            }
        }
        final double stdDev = numGoodPixels > 1 ? Math.sqrt(sumSqrDev / (numGoodPixels - 1)) : 0.0;

        if (filteredMeanCoeff > 0) {
            // Step 2: Compute filteredMean
            double filteredSum = 0;
            int numFilteredPixels = 0;
            final double lowerDistrLimit = mean - filteredMeanCoeff * stdDev;
            final double upperDistrLimit = mean + filteredMeanCoeff * stdDev;
            for (int i = 0; i < values.length; i++) {
                final float value = values[i];
                if (!Float.isNaN(value) && isGoodPixel(maskValues, i)) {
                    if (value > lowerDistrLimit && value < upperDistrLimit) {
                        numFilteredPixels++;
                        filteredSum += value;
                    }
                }
            }
            final double filteredMean = numFilteredPixels > 0 ? filteredSum / numFilteredPixels : Float.NaN;

            // Step 4: Compute filteredStdDev
            double filteredSumSqrDev = 0;
            for (int i = 0; i < values.length; i++) {
                final float value = values[i];
                if (!Float.isNaN(value) && isGoodPixel(maskValues, i)) {
                    if (value > lowerDistrLimit && value < upperDistrLimit) {
                        filteredSumSqrDev += (filteredMean - value) * (filteredMean - value);
                    }
                }
            }
            final double filteredStdDev = numFilteredPixels > 1 ? Math.sqrt(filteredSumSqrDev / (numFilteredPixels - 1)) : 0.0;

            // Done!
            return new AggregatedNumber(numTotalPixels,
                                        min,
                                        max,
                                        mean,
                                        stdDev,
                                        numGoodPixels,
                                        filteredMean,
                                        filteredStdDev,
                                        numFilteredPixels);
        } else {
            return new AggregatedNumber(numTotalPixels,
                                        min,
                                        max,
                                        mean,
                                        stdDev,
                                        numGoodPixels,
                                        mean,  // = filteredMean
                                        stdDev,  //  = filteredStdDev
                                        numGoodPixels);  // = numFilteredPixels
        }
    }

    private static boolean isGoodPixel(int[] maskValues, int i) {
        return maskValues == null || maskValues[i] != 0;
    }

    private static int getCommonArrayValueLength(Object[] attributeValues) {
        int commonLength = -1;
        for (Object attributeValue : attributeValues) {
            if (attributeValue != null && attributeValue.getClass().isArray()) {
                int length = Array.getLength(attributeValue);
                if (commonLength >= 0 && length != commonLength) {
                    throw new IllegalArgumentException("Record with varying array lengths detected. Expected " + commonLength + ", found " + length);
                }
                commonLength = length;
                if (!(attributeValue instanceof int[] || attributeValue instanceof float[])) {
                    throw new IllegalArgumentException("Records with array values can only be of type int[] or float[].");
                }
            }
        }
        if (commonLength == 0) {
            throw new IllegalArgumentException("Record with zero-length arrays.");
        }
        return commonLength;
    }

    private boolean isRecordAccepted(Record record) {
        for (RecordFilter filter : recordFilters) {
            if (!filter.accept(record)) {
                return false;
            }
        }
        return true;
    }

}
