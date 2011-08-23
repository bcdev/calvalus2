package com.bc.calvalus.processing.ma;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RecordTransformer {

    private final int maskAttributeIndex;
    private final AggregatedNumberFilter[] filters;

    public RecordTransformer(int maskAttributeIndex, AggregatedNumberFilter... filters) {
        this.maskAttributeIndex = maskAttributeIndex;
        this.filters = filters;
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
                resultingRecords.add(new DefaultRecord(record.getCoordinate(), record.getTime(), values));
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
                for (AggregatedNumberFilter filter : filters) {
                    if (!filter.accept(valueIndex, aggregatedNumber)) {
                        return null;
                    }
                }
                aggregatedValues[valueIndex] = aggregatedNumber;
            } else {
                aggregatedValues[valueIndex] = attributeValue;
            }
        }
        return new DefaultRecord(record.getCoordinate(), record.getTime(), aggregatedValues);
    }

    protected AggregatedNumber aggregate(int[] array, int[] maskValues) {
        float[] floats = new float[array.length];
        for (int i = 0; i < floats.length; i++) {
            floats[i] = array[i];
        }
        return aggregate(floats, maskValues);
    }

    protected AggregatedNumber aggregate(float[] array, int[] maskValues) {

        // Step 1: Compute mean
        double sum = 0;
        int numGoodPixels = 0;
        int numTotalPixels = 0;
        for (int i = 0; i < array.length; i++) {
            final float value = array[i];
            if (!Float.isNaN(value)) {
                numTotalPixels++;
                if (isGoodPixel(maskValues, i)) {
                    numGoodPixels++;
                    sum += value;
                }
            }
        }
        final double mean = numGoodPixels > 0 ? sum / numGoodPixels : Float.NaN;

        // Step 2: Compute stdDev
        double sumSqrDev = 0;
        for (int i = 0; i < array.length; i++) {
            final float value = array[i];
            if (!Float.isNaN(value) && isGoodPixel(maskValues, i)) {
                sumSqrDev += (mean - value) * (mean - value);
            }
        }
        final double stdDev = numGoodPixels > 1 ? Math.sqrt(sumSqrDev / (numGoodPixels - 1)) : 0.0;

        // Step 2: Compute filteredMean
        double filteredSum = 0;
        int numFilteredPixels = 0;
        final double lowerDistrLimit = mean - 1.5 * stdDev;
        final double upperDistrLimit = mean + 1.5 * stdDev;
        for (int i = 0; i < array.length; i++) {
            final float value = array[i];
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
        for (int i = 0; i < array.length; i++) {
            final float value = array[i];
            if (!Float.isNaN(value) && isGoodPixel(maskValues, i)) {
                if (value > lowerDistrLimit && value < upperDistrLimit) {
                    filteredSumSqrDev += (filteredMean - value) * (filteredMean - value);
                }
            }
        }
        final double filteredStdDev = numFilteredPixels > 1 ? Math.sqrt(filteredSumSqrDev / (numFilteredPixels - 1)) : 0.0;

        // Done!
        return new AggregatedNumber(numTotalPixels, numGoodPixels, numFilteredPixels,
                                    (float) mean, (float) stdDev, (float) filteredMean, (float) filteredStdDev);
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

}
