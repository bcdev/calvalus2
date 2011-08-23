package com.bc.calvalus.processing.ma;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RecordTransformer {

    private final int maskAttributeIndex;

    public RecordTransformer(int maskAttributeIndex) {
        this.maskAttributeIndex = maskAttributeIndex;
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
            if (attributeValue == maskValues) {
                aggregatedValues[valueIndex] = aggregate(maskValues, null);
            } else  if (attributeValue instanceof float[]) {
                aggregatedValues[valueIndex] = aggregate((float[]) attributeValue, maskValues);
            } else if (attributeValue instanceof int[]) {
                aggregatedValues[valueIndex] = aggregate((int[]) attributeValue, maskValues);
            } else {
                aggregatedValues[valueIndex] = attributeValue;
            }
        }
        return new DefaultRecord(record.getCoordinate(), record.getTime(), aggregatedValues);
    }

    protected AggregatedNumber aggregate(float[] array, int[] maskValues) {
        float sum = 0F;
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
        return new AggregatedNumber(sum, numGoodPixels, numTotalPixels);
    }

    protected AggregatedNumber aggregate(int[] array, int[] maskValues) {
        float sum = 0F;
        int numGoodPixels = 0;
        for (int i = 0; i < array.length; i++) {
            if (isGoodPixel(maskValues, i)) {
                numGoodPixels++;
                sum += array[i];
            }
        }
        return new AggregatedNumber(sum, numGoodPixels, array.length);
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
