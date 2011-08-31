package com.bc.calvalus.processing.ma;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.bc.calvalus.processing.ma.PixelExtractor.AGGREGATION_PREFIX;

public class RecordTransformer {

    private static final String SUFFIX_MEAN = "_mean";
    private static final String SUFFIX_SIGMA = "_sigma";
    private static final String SUFFIX_N = "_n";

    private final int maskAttributeIndex;
    private final double filteredMeanCoeff;
    private final RecordFilter[] recordFilters;

    public RecordTransformer(int maskAttributeIndex, double filteredMeanCoeff, RecordFilter... recordFilters) {
        this.maskAttributeIndex = maskAttributeIndex;
        this.filteredMeanCoeff = filteredMeanCoeff;
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

    private int[] getMaskValues(Object[] attributeValues) {
        if (maskAttributeIndex != -1) {
            return (int[]) attributeValues[maskAttributeIndex];
        } else {
            return null;
        }
    }

    public Record aggregate(Record record) {
        final Object[] attributeValues = record.getAttributeValues();
        if (getCommonArrayValueLength(attributeValues) == -1) {
            return record;
        }
        final int[] maskValues = getMaskValues(attributeValues);
        final Object[] aggregatedValues = new Object[attributeValues.length];
        for (int valueIndex = 0; valueIndex < aggregatedValues.length; valueIndex++) {
            final Object attributeValue = attributeValues[valueIndex];
            final AggregatedNumber aggregatedNumber;
            if (attributeValue != null && attributeValue == maskValues) {
                aggregatedNumber = aggregate(maskValues, null);
            } else if (attributeValue instanceof float[]) {
                aggregatedNumber = aggregate((float[]) attributeValue, maskValues);
            } else if (attributeValue instanceof int[]) {
                aggregatedNumber = aggregate((int[]) attributeValue, maskValues);
            } else {
                aggregatedNumber = null;
            }
            if (aggregatedNumber != null) {
                aggregatedValues[valueIndex] = aggregatedNumber;
            } else {
                aggregatedValues[valueIndex] = attributeValue;
            }
        }
        final DefaultRecord aggregatedRecord = new DefaultRecord(record.getLocation(), record.getTime(), aggregatedValues);
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
        double sumSigma = 0;
        for (int i = 0; i < values.length; i++) {
            final float value = values[i];
            if (!Float.isNaN(value) && isGoodPixel(maskValues, i)) {
                sumSigma += (mean - value) * (mean - value);
            }
        }
        final double sigma = numGoodPixels > 1 ? Math.sqrt(sumSigma / (numGoodPixels - 1)) : 0.0;

        // If we don't want to filter or we can't, then we are done.
        if (filteredMeanCoeff <= 0.0 || Math.abs(sigma) < 1E-10) {
            return new AggregatedNumber(numGoodPixels, numTotalPixels, 0, min, max, mean, sigma);
        }

        // Step 3: Compute filteredMean

        final double lowerBound = mean - filteredMeanCoeff * sigma;
        final double upperBound = mean + filteredMeanCoeff * sigma;

        numGoodPixels = 0;
        int numFilteredPixels = 0;

        double filteredSum = 0;
        double filteredMin = +Float.MAX_VALUE;
        double filteredMax = -Float.MAX_VALUE;

        for (int i = 0; i < values.length; i++) {
            final float value = values[i];
            if (!Float.isNaN(value) && isGoodPixel(maskValues, i)) {
                if (value >= lowerBound && value <= upperBound) {
                    filteredSum += value;
                    filteredMin = Math.min(filteredMin, value);
                    filteredMax = Math.max(filteredMax, value);
                    numGoodPixels++;
                } else {
                    numFilteredPixels++;
                }
            }
        }
        final double filteredMean = numGoodPixels > 0 ? filteredSum / numGoodPixels : Float.NaN;

        // Step 4: Compute filteredStdDev
        double filteredSumSigma = 0;
        for (int i = 0; i < values.length; i++) {
            final float value = values[i];
            if (!Float.isNaN(value) && isGoodPixel(maskValues, i)) {
                if (value > lowerBound && value < upperBound) {
                    filteredSumSigma += (filteredMean - value) * (filteredMean - value);
                }
            }
        }
        final double filteredSigma = numGoodPixels > 1 ? Math.sqrt(filteredSumSigma / (numGoodPixels - 1)) : 0.0;

        // Done!
        return new AggregatedNumber(numGoodPixels, numTotalPixels, numFilteredPixels,
                                    filteredMin, filteredMax, filteredMean, filteredSigma);
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

    public static String[] getHeaderForAggregatedRecords(String[] attributeNames) {
        return transformHeader(attributeNames, true);
    }

    public static String[] getHeaderForExplodedRecords(String[] attributeNames) {
        return transformHeader(attributeNames, false);
    }

    private static String[] transformHeader(String[] attributeNames, boolean makeStatColumns) {
        ArrayList<String> strings = new ArrayList<String>(attributeNames.length * (makeStatColumns ? 3 : 1));
        for (String attributeName : attributeNames) {
            if (attributeName.startsWith(AGGREGATION_PREFIX)) {
                String name = attributeName.substring(AGGREGATION_PREFIX.length());
                if (makeStatColumns) {
                    strings.add(name + SUFFIX_MEAN);
                    strings.add(name + SUFFIX_SIGMA);
                    strings.add(name + SUFFIX_N);
                } else {
                    strings.add(name);
                }
            } else {
                strings.add(attributeName);
            }
        }
        return strings.toArray(new String[strings.size()]);
    }

}
