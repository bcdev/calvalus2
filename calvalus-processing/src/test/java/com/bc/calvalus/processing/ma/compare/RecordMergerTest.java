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

import com.bc.calvalus.processing.ma.DefaultHeader;
import com.bc.calvalus.processing.ma.RecordProcessor;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class RecordMergerTest {

    private RecordMerger recordMerger;
    private TracingRecordProcessor rpAll;
    private TracingRecordProcessor rpCBQ;
    private TracingRecordProcessor rpIBQp1;
    private TracingRecordProcessor rpIBQp2;
    private List<IndexedRecordWritable> headerValues;
    private String[] insituAttributeNames;
    private Object[] insituAttributeValues;

    @Before
    public void setUp() throws Exception {
        rpAll = new TracingRecordProcessor();
        rpCBQ = new TracingRecordProcessor();
        rpIBQp1 = new TracingRecordProcessor();
        rpIBQp2 = new TracingRecordProcessor();
        String[] identifier = new String[]{"p1", "p2"};
        recordMerger = new RecordMerger(identifier, rpAll, rpCBQ, rpIBQp1, rpIBQp2);

        insituAttributeNames = new String[]{"a", "b"};
        insituAttributeValues = new Integer[]{1, 2};

        headerValues = new ArrayList<IndexedRecordWritable>();
        headerValues.add(new IndexedRecordWritable(
                0,
                new String[]{"p1_m", "p1_n"},
                new String[]{DefaultHeader.ANNOTATION_EXCLUSION_REASON}
        ));
        headerValues.add(new IndexedRecordWritable(
                1,
                new String[]{"p2_o", "p2_q"},
                new String[]{DefaultHeader.ANNOTATION_EXCLUSION_REASON}
        ));
    }

    @Test
    public void processHeader() throws Exception {
        recordMerger.processHeader(insituAttributeNames, headerValues);
        String[] expected = {"insitu_a", "insitu_b", "p1_m", "p1_n", "p2_o", "p2_q"};
        assertArrayEquals(expected, rpAll.headerAttributes.toArray());
        assertArrayEquals(expected, rpCBQ.headerAttributes.toArray());
        assertArrayEquals(expected, rpIBQp1.headerAttributes.toArray());
        assertArrayEquals(expected, rpIBQp2.headerAttributes.toArray());

        expected = new String[]{""};
        assertArrayEquals(expected, rpAll.headerAnnotations.toArray());
        assertArrayEquals(expected, rpCBQ.headerAnnotations.toArray());
        assertArrayEquals(expected, rpIBQp1.headerAnnotations.toArray());
        assertArrayEquals(expected, rpIBQp2.headerAnnotations.toArray());
    }

    @Test
    public void processAllGood() throws Exception {
        List<IndexedRecordWritable> data;
        data = new ArrayList<IndexedRecordWritable>();
        data.add(new IndexedRecordWritable(
                0,
                new Integer[]{11, 12},
                new String[]{""}
        ));
        data.add(new IndexedRecordWritable(
                1,
                new Integer[]{21, 22},
                new String[]{""}
        ));

        recordMerger.processHeader(insituAttributeNames, headerValues);
        recordMerger.processData("key", insituAttributeValues, data);

        Integer[] expectedInts = {1, 2, 11, 12, 21, 22};
        assertArrayEquals(expectedInts, rpAll.dataAttributes.toArray());
        assertArrayEquals(expectedInts, rpCBQ.dataAttributes.toArray());
        assertArrayEquals(expectedInts, rpIBQp1.dataAttributes.toArray());
        assertArrayEquals(expectedInts, rpIBQp2.dataAttributes.toArray());

        String[] expectedStrings = new String[]{""};
        assertArrayEquals(expectedStrings, rpAll.dataAnnotations.toArray());
        assertArrayEquals(expectedStrings, rpCBQ.dataAnnotations.toArray());
        assertArrayEquals(expectedStrings, rpIBQp1.dataAnnotations.toArray());
        assertArrayEquals(expectedStrings, rpIBQp2.dataAnnotations.toArray());
    }

    @Test
    public void processOneMissing() throws Exception {
        List<IndexedRecordWritable> data;
        data = new ArrayList<IndexedRecordWritable>();
        data.add(new IndexedRecordWritable(
                1,
                new Integer[]{21, 22},
                new String[]{""}
        ));
        recordMerger.processHeader(insituAttributeNames, headerValues);
        recordMerger.processData("key", insituAttributeValues, data);

        Integer[] expectedInts = {1, 2, null, null, 21, 22};
        assertEquals(6, rpAll.dataAttributes.size());
        assertEquals(0, rpCBQ.dataAttributes.size());
        assertEquals(0, rpIBQp1.dataAttributes.size());
        assertEquals(6, rpIBQp2.dataAttributes.size());
        assertArrayEquals(expectedInts, rpAll.dataAttributes.toArray());
        assertArrayEquals(expectedInts, rpIBQp2.dataAttributes.toArray());

        String[] expectedStrings = new String[]{""};
        assertEquals(1, rpAll.dataAnnotations.size());
        assertEquals(0, rpCBQ.dataAnnotations.size());
        assertEquals(0, rpIBQp1.dataAnnotations.size());
        assertEquals(1, rpIBQp2.dataAnnotations.size());
        assertArrayEquals(expectedStrings, rpAll.dataAnnotations.toArray());
        assertArrayEquals(expectedStrings, rpIBQp2.dataAnnotations.toArray());
    }

    @Test
    public void processOneBad() throws Exception {
        List<IndexedRecordWritable> data;
        data = new ArrayList<IndexedRecordWritable>();
        data.add(new IndexedRecordWritable(
                0,
                new Integer[]{11, 12},
                new String[]{"bad"}
        ));
        data.add(new IndexedRecordWritable(
                1,
                new Integer[]{21, 22},
                new String[]{""}
        ));
        recordMerger.processHeader(insituAttributeNames, headerValues);
        recordMerger.processData("key", insituAttributeValues, data);

        Integer[] expectedInts = {1, 2, null, null, 21, 22};
        assertEquals(6, rpAll.dataAttributes.size());
        assertEquals(0, rpCBQ.dataAttributes.size());
        assertEquals(0, rpIBQp1.dataAttributes.size());
        assertEquals(6, rpIBQp2.dataAttributes.size());
        assertArrayEquals(expectedInts, rpAll.dataAttributes.toArray());
        assertArrayEquals(expectedInts, rpIBQp2.dataAttributes.toArray());

        String[] expectedStrings = new String[]{""};
        assertEquals(1, rpAll.dataAnnotations.size());
        assertEquals(0, rpCBQ.dataAnnotations.size());
        assertEquals(0, rpIBQp1.dataAnnotations.size());
        assertEquals(1, rpIBQp2.dataAnnotations.size());
        assertArrayEquals(expectedStrings, rpAll.dataAnnotations.toArray());
        assertArrayEquals(expectedStrings, rpIBQp2.dataAnnotations.toArray());
    }

    @Test
    public void processAllBad() throws Exception {
        List<IndexedRecordWritable> data;
        data = new ArrayList<IndexedRecordWritable>();
        data.add(new IndexedRecordWritable(
                0,
                new Integer[]{11, 12},
                new String[]{"bad"}
        ));
        data.add(new IndexedRecordWritable(
                1,
                new Integer[]{21, 22},
                new String[]{"worse"}
        ));
        recordMerger.processHeader(insituAttributeNames, headerValues);
        recordMerger.processData("key", insituAttributeValues, data);

        assertEquals(0, rpAll.dataAttributes.size());
        assertEquals(0, rpCBQ.dataAttributes.size());
        assertEquals(0, rpIBQp1.dataAttributes.size());
        assertEquals(0, rpIBQp2.dataAttributes.size());

        assertEquals(0, rpAll.dataAnnotations.size());
        assertEquals(0, rpCBQ.dataAnnotations.size());
        assertEquals(0, rpIBQp1.dataAnnotations.size());
        assertEquals(0, rpIBQp2.dataAnnotations.size());
    }

    @Test
    public void processAllMissing() throws Exception {
        List<IndexedRecordWritable> data;
        data = new ArrayList<IndexedRecordWritable>();
        recordMerger.processHeader(insituAttributeNames, headerValues);
        recordMerger.processData("key", insituAttributeValues, data);

        assertEquals(0, rpAll.dataAttributes.size());
        assertEquals(0, rpCBQ.dataAttributes.size());
        assertEquals(0, rpIBQp1.dataAttributes.size());
        assertEquals(0, rpIBQp2.dataAttributes.size());

        assertEquals(0, rpAll.dataAnnotations.size());
        assertEquals(0, rpCBQ.dataAnnotations.size());
        assertEquals(0, rpIBQp1.dataAnnotations.size());
        assertEquals(0, rpIBQp2.dataAnnotations.size());
    }

    private static class TracingRecordProcessor implements RecordProcessor {

        List<Object> headerAttributes = new ArrayList<Object>();
        List<Object> headerAnnotations = new ArrayList<Object>();

        List<Object> dataAttributes = new ArrayList<Object>();
        List<Object> dataAnnotations = new ArrayList<Object>();

        @Override
        public void processHeaderRecord(Object[] attributeNames, Object[] annotationNames) throws IOException {
            Collections.addAll(headerAttributes, attributeNames);
            Collections.addAll(headerAnnotations, annotationNames);
        }

        @Override
        public void processDataRecord(String key, Object[] recordValues, Object[] annotationValues) throws IOException {
            Collections.addAll(dataAttributes, recordValues);
            Collections.addAll(dataAnnotations, annotationValues);
        }

        @Override
        public void finalizeRecordProcessing() throws IOException {
        }
    }
}
