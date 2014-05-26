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

package com.bc.calvalus.processing.vc;

import com.bc.calvalus.processing.ma.DefaultHeader;
import com.bc.calvalus.processing.ma.DefaultRecord;
import com.bc.calvalus.processing.ma.Header;
import com.bc.calvalus.processing.ma.Record;
import com.bc.calvalus.processing.ma.TestHeader;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.*;

public class MergedRecordSourceTest {

    private Header headerA;
    private Header headerB;
    private NamedRecordSource sourceA;
    private NamedRecordSource sourceB;
    private NamedRecordSource level2;

    @Before
    public void setUp() throws Exception {
        headerA = new TestHeader("1", "2", "*3");
        headerB = new TestHeader("a", "b", "c");
        Record recordA = new DefaultRecord(1, null, null, new Object[]{21, 42, 63}, new Object[]{"Annotation_A"});
        Record recordB = new DefaultRecord(2, null, null, new Object[]{3, 6, 9}, new Object[]{"Annotation_B"});
        Record recordL2 = new DefaultRecord(3, null, null, new Object[]{1, 2, 3}, new Object[]{"Annotation_L2"});

        sourceA = new NamedRecordSource("A", headerA, Arrays.asList(recordA));
        sourceB = new NamedRecordSource("B", headerB, Arrays.asList(recordB));
        level2 = new NamedRecordSource("L2", headerB, Arrays.asList(recordL2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeZero() throws Exception {
        new MergedRecordSource(level2, Collections.<NamedRecordSource>emptyList());
    }

    @Test
    public void testMergeOne() throws Exception {
        MergedRecordSource merged = new MergedRecordSource(level2, Arrays.asList(sourceA));

        Header header = merged.getHeader();
        assertNotNull(header);
        String[] attributeNames = header.getAttributeNames();
        assertNotNull(attributeNames);
        assertEquals(headerA.getAttributeNames().length, attributeNames.length);
        assertArrayEquals(new String[]{"A1", "A2", "*A3"}, attributeNames);
        assertArrayEquals(new String[]{DefaultHeader.ANNOTATION_EXCLUSION_REASON}, header.getAnnotationNames());

        Iterator<Record> iterator = merged.getRecords().iterator();
        assertTrue(iterator.hasNext());
        Record record = iterator.next();
        assertFalse(iterator.hasNext());

        assertNotNull(record);
        assertArrayEquals(new Object[]{21, 42, 63}, record.getAttributeValues());
        assertArrayEquals(new Object[]{"Annotation_L2"}, record.getAnnotationValues());
    }

    @Test
    public void testMergeTwo() throws Exception {
        MergedRecordSource merged = new MergedRecordSource(level2, Arrays.asList(sourceA, sourceB));

        Header header = merged.getHeader();
        assertNotNull(header);
        String[] attributeNames = header.getAttributeNames();
        assertNotNull(attributeNames);
        assertEquals(headerA.getAttributeNames().length + headerB.getAttributeNames().length, attributeNames.length);
        assertArrayEquals(new String[]{"A1", "A2", "*A3", "Ba", "Bb", "Bc"}, attributeNames);
        assertArrayEquals(new String[]{DefaultHeader.ANNOTATION_EXCLUSION_REASON}, header.getAnnotationNames());

        Iterator<Record> iterator = merged.getRecords().iterator();
        assertTrue(iterator.hasNext());
        Record record = iterator.next();
        assertFalse(iterator.hasNext());

        assertNotNull(record);
        assertArrayEquals(new Object[]{21, 42, 63, 3, 6, 9}, record.getAttributeValues());
        assertArrayEquals(new Object[]{"Annotation_L2"}, record.getAnnotationValues());
    }
}