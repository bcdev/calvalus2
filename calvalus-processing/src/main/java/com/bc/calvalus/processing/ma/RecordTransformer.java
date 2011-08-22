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
            if (maskValues == null || maskValues[recordIndex] == 1) {
                Object[] values = new Object[attributeValues.length];
                for (int valueIndex = 0; valueIndex < values.length; valueIndex++) {
                    Object o = attributeValues[valueIndex];
                    if (o != null && o.getClass().isArray()) {
                        values[valueIndex] = Array.get(o, recordIndex);
                    } else {
                        values[valueIndex] = o;
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
        Object[] values = new Object[attributeValues.length];
        for (int valueIndex = 0; valueIndex < values.length; valueIndex++) {
            Object o = attributeValues[valueIndex];
            if (o instanceof float[]) {
                values[valueIndex] = aggregate((float[]) o);
            } else if (o instanceof int[]) {
                values[valueIndex] = aggregate((int[]) o);
            } else {
                values[valueIndex] = o;
            }
        }
        return new DefaultRecord(record.getCoordinate(), record.getTime(), values);
    }

    protected Number aggregate(float[] array) {
        float sum = 0F;
        for (float value : array) {
            sum += value;
        }
        return sum / array.length;
    }

    protected Number aggregate(int[] array) {
        float sum = 0F;
        for (float value : array) {
            sum += value;
        }
        return sum / array.length;
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


    public static class AggregatedFloat extends Number {
        float mean;
        float stddev;
        int ngp;
        int ntp;


        @Override
        public int intValue() {
            return Math.round(mean);
        }

        @Override
        public long longValue() {
            return Math.round(mean);
        }

        @Override
        public float floatValue() {
            return mean;
        }

        @Override
        public double doubleValue() {
            return mean;
        }

        @Override
        public String toString() {
            return String.valueOf(mean);
        }
    }

}
