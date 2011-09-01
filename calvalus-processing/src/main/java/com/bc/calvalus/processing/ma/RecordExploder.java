package com.bc.calvalus.processing.ma;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RecordExploder {

    private final int maskAttributeIndex;
    private final RecordFilter[] recordFilters;

    public RecordExploder(int maskAttributeIndex,  RecordFilter ... recordFilters) {
        this.maskAttributeIndex = maskAttributeIndex;
        this.recordFilters = recordFilters;
    }

    public List<Record> explode(Record record) {
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

    private boolean isRecordAccepted(DefaultRecord resultingRecord) {
        for (int i = 0; i < recordFilters.length; i++) {
            RecordFilter recordFilter = recordFilters[i];
            if (!recordFilter.accept(resultingRecord))  {
                return false;
            }
        }
        return true;
    }

    private int[] getMaskValues(Object[] attributeValues) {
        if (maskAttributeIndex != -1) {
            return (int[]) attributeValues[maskAttributeIndex];
        } else {
            return null;
        }
    }

    private static boolean isGoodPixel(int[] maskValues, int i) {
        return maskValues == null || maskValues[i] != 0;
    }

    static int getCommonArrayValueLength(Object[] attributeValues) {
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

    public static String[] getHeaderForAggregatedRecords(String[] attributeNames) {
        return CsvRecordWriter.transformHeader(attributeNames, true);
    }

    public static String[] getHeaderForExplodedRecords(String[] attributeNames) {
        return CsvRecordWriter.transformHeader(attributeNames, false);
    }

}
