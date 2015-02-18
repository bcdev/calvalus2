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

package com.bc.calvalus.processing.ma.compare;

import com.bc.calvalus.processing.ma.AggregatedNumber;
import com.bc.calvalus.processing.ma.RecordProcessor;
import com.bc.ceres.core.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RecordMerger {

    private final String[] identifiers;
    private final RecordProcessor all;
    private final RecordProcessor common;
    private final RecordProcessor[] individuals;

    private int[] numAttributes;
    private int[] startAttributeIndex;
    private int numAttributeValues;
    private static final Object[] ANNOTATION_VALUES = new Object[]{""};

    public RecordMerger(String[] identifiers, RecordProcessor all, RecordProcessor common, RecordProcessor... individuals) {
        Assert.argument(identifiers.length == individuals.length);
        this.identifiers = identifiers;
        this.all = all;
        this.common = common;
        this.individuals = individuals;
        this.numAttributes = new int[identifiers.length + 1];
        this.startAttributeIndex = new int[identifiers.length + 1];
    }

    void processHeader(String[] insituAttributeNames, Iterable<IndexedRecordWritable> records) throws IOException {
        List<Object> attributeNameList = new ArrayList<>();
        startAttributeIndex[0] = 0;
        numAttributes[0] = insituAttributeNames.length;
        startAttributeIndex[1] = startAttributeIndex[0] + insituAttributeNames.length;
        for (String insituAttributeName : insituAttributeNames) {
            attributeNameList.add("insitu_" + insituAttributeName);
        }
        int index = 1;
        for (IndexedRecordWritable recordWritable : records) {
            Object[] attributeNames = recordWritable.getAttributeValues();
            numAttributes[index] = attributeNames.length;
            index++;
            if (index < startAttributeIndex.length) {
                startAttributeIndex[index] = startAttributeIndex[index - 1] + attributeNames.length;
            }
            Collections.addAll(attributeNameList, attributeNames);
        }
        Object[] attributeValues = attributeNameList.toArray(new Object[attributeNameList.size()]);

        numAttributeValues = startAttributeIndex[numAttributes.length - 1] + numAttributes[numAttributes.length - 1];

        all.processHeaderRecord(attributeValues, ANNOTATION_VALUES);
        common.processHeaderRecord(attributeValues, ANNOTATION_VALUES);
        for (RecordProcessor individual : individuals) {
            individual.processHeaderRecord(attributeValues, ANNOTATION_VALUES);
        }
    }

    void processData(String key, Object[] insituAttributeValues, Iterable<IndexedRecordWritable> records) throws IOException {
        Object[] mergedAttributeValues = new Object[numAttributeValues];

        System.arraycopy(insituAttributeValues, 0, mergedAttributeValues, startAttributeIndex[0],
                         insituAttributeValues.length);

        boolean[] isRecordGood = new boolean[identifiers.length];

        boolean containsGoodRecord = false;
        int commonDataArrayLength = -1;
        boolean recordWithVaryingArrayLengths = false;
        for (IndexedRecordWritable recordWritable : records) {
            int identifierIndex = recordWritable.getIdentifierIndex() + 1;
            Object[] attributeValues = recordWritable.getAttributeValues();
            Object[] annotationValues = recordWritable.getAnnotationValues();

            int dataArrayLength = getCommonDataArrayLength(attributeValues);
            if (commonDataArrayLength == -1) {
                commonDataArrayLength = dataArrayLength;
            } else if (commonDataArrayLength != dataArrayLength) {
                recordWithVaryingArrayLengths = true;
            }

            boolean goodRecord = annotationValues.length == 0 || !(annotationValues[0] instanceof String) || ((String) annotationValues[0]).isEmpty();
            if (goodRecord) {
                System.arraycopy(attributeValues, 0, mergedAttributeValues, startAttributeIndex[identifierIndex],
                                 attributeValues.length);
                if (identifierIndex > 0) {
                    isRecordGood[identifierIndex - 1] = true;
                    containsGoodRecord = true;
                }
            }
        }

        if (containsGoodRecord && !recordWithVaryingArrayLengths) {
            boolean allRecordsGood = true;
            all.processDataRecord(key, mergedAttributeValues, ANNOTATION_VALUES);
            for (int i = 0; i < individuals.length; i++) {
                if (isRecordGood[i]) {
                    individuals[i].processDataRecord(key, mergedAttributeValues, ANNOTATION_VALUES);
                } else {
                    allRecordsGood = false;
                }
            }
            if (allRecordsGood) {
                common.processDataRecord(key, mergedAttributeValues, ANNOTATION_VALUES);
            }
        }
    }

    private int getCommonDataArrayLength(Object[] attributeValues) {
        for (Object attributeValue : attributeValues) {
            if (attributeValue instanceof AggregatedNumber) {
                AggregatedNumber value = (AggregatedNumber) attributeValue;
                if (value.data != null) {
                    return value.data.length;
                }
            }
        }
        return -1;
    }

    public void finalizeRecordProcessing() throws IOException {
        all.finalizeRecordProcessing();
        common.finalizeRecordProcessing();
        for (RecordProcessor individual : individuals) {
            individual.finalizeRecordProcessing();
        }
    }
}
