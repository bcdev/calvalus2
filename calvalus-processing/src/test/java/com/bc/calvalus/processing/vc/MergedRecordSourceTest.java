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

    private NamedRecordSource insitu;
    private NamedRecordSource sourceA;
    private NamedRecordSource sourceB;
    private NamedRecordSource level2;

    @Before
    public void setUp() throws Exception {
        Header headerA = new TestHeader("1", "2", "*3");
        Header headerB = new TestHeader("a", "b", "c");
        Record insitu_1 = new DefaultRecord(1, null, null, new Object[]{42});
        Record insitu_2 = new DefaultRecord(2, null, null, new Object[]{43});
        Record recordA_1 = new DefaultRecord(1, null, null, new Object[]{21.1, 42.1, 63.1}, new Object[]{"Annotation_A"});
        Record recordA_2 = new DefaultRecord(2, null, null, new Object[]{21.2, 42.2, 63.2}, new Object[]{"Annotation_A"});
        Record recordB_1 = new DefaultRecord(2, null, null, new Object[]{3.2, 6.2, 9.2}, new Object[]{"Annotation_B"});
        Record recordB_2 = new DefaultRecord(1, null, null, new Object[]{3.1, 6.1, 9.1}, new Object[]{"Annotation_B"});
        Record recordL2_1 = new DefaultRecord(1, null, null, new Object[]{1.1, 2.1, 3.1}, new Object[]{"Annotation_L2"});
        Record recordL2_2 = new DefaultRecord(2, null, null, new Object[]{1.2, 2.2, 3.2}, new Object[]{"Annotation_L2"});

        insitu = new NamedRecordSource("insitu", headerA, Arrays.asList(insitu_1, insitu_2));
        sourceA = new NamedRecordSource("A", headerA, Arrays.asList(recordA_1, recordA_2));
        sourceB = new NamedRecordSource("B", headerB, Arrays.asList(recordB_1, recordB_2));
        level2 = new NamedRecordSource("L2", headerB, Arrays.asList(recordL2_1, recordL2_2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMergeZero() throws Exception {
        new MergedRecordSource(insitu, level2, Collections.emptyList());
    }

    @Test
    public void testMergeOne() throws Exception {
        MergedRecordSource merged = new MergedRecordSource(insitu, level2, Arrays.asList(sourceA));

        Header header = merged.getHeader();
        assertNotNull(header);
        String[] attributeNames = header.getAttributeNames();
        assertNotNull(attributeNames);
        assertEquals(6, attributeNames.length);
        assertArrayEquals(new String[]{"A1", "A2", "*A3", "L2a", "L2b", "L2c"}, attributeNames);
        assertArrayEquals(new String[]{DefaultHeader.ANNOTATION_EXCLUSION_REASON}, header.getAnnotationNames());

        Iterator<Record> iterator = merged.getRecords().iterator();
        assertTrue(iterator.hasNext());
        Record record1 = iterator.next();
        assertTrue(iterator.hasNext());
        Record record2 = iterator.next();
        assertFalse(iterator.hasNext());

        assertNotNull(record1);
        assertArrayEquals(new Object[]{21.1, 42.1, 63.1, 1.1, 2.1, 3.1}, record1.getAttributeValues());
        assertArrayEquals(new Object[]{"Annotation_L2"}, record1.getAnnotationValues());

        assertNotNull(record2);
        assertArrayEquals(new Object[]{21.2, 42.2, 63.2, 1.2, 2.2, 3.2}, record2.getAttributeValues());
        assertArrayEquals(new Object[]{"Annotation_L2"}, record2.getAnnotationValues());
    }

    @Test
    public void testMergeTwo() throws Exception {
        MergedRecordSource merged = new MergedRecordSource(insitu, level2, Arrays.asList(sourceA, sourceB));

        Header header = merged.getHeader();
        assertNotNull(header);
        String[] attributeNames = header.getAttributeNames();
        assertNotNull(attributeNames);
        assertEquals(9, attributeNames.length);
        assertArrayEquals(new String[]{"A1", "A2", "*A3", "Ba", "Bb", "Bc", "L2a", "L2b", "L2c"}, attributeNames);
        assertArrayEquals(new String[]{DefaultHeader.ANNOTATION_EXCLUSION_REASON}, header.getAnnotationNames());

        Iterator<Record> iterator = merged.getRecords().iterator();
        assertTrue(iterator.hasNext());
        Record record1 = iterator.next();
        assertTrue(iterator.hasNext());
        Record record2 = iterator.next();
        assertFalse(iterator.hasNext());

        assertNotNull(record1);
        assertArrayEquals(new Object[]{21.1, 42.1, 63.1, 3.1, 6.1, 9.1, 1.1, 2.1, 3.1}, record1.getAttributeValues());
        assertArrayEquals(new Object[]{"Annotation_L2"}, record1.getAnnotationValues());

        assertNotNull(record2);
        assertArrayEquals(new Object[]{21.2, 42.2, 63.2, 3.2, 6.2, 9.2, 1.2, 2.2, 3.2}, record2.getAttributeValues());
        assertArrayEquals(new Object[]{"Annotation_L2"}, record2.getAnnotationValues());

    }
}