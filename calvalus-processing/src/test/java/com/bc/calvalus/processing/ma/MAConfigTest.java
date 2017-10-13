/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.ma;


import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * The configuration for the match-up processing.
 *
 * @author MarcoZ
 * @author Norman
 */
public class MAConfigTest {

    @Test
    public void testFromXmlSetsDefaults() throws Exception {
        MAConfig maConfig = MAConfig.fromXml("<parameters/>");

        assertEquals(null, maConfig.getRecordSourceSpiClassName());
        assertEquals(null, maConfig.getRecordSourceUrl());
        assertEquals(1.5, maConfig.getFilteredMeanCoeff(), 1E-6);
        assertEquals(5, maConfig.getMacroPixelSize());
        assertEquals("3.0", maConfig.getMaxTimeDifference());
        assertEquals(null, maConfig.getGoodPixelExpression());
        assertEquals(null, maConfig.getGoodRecordExpression());
        assertEquals("dd-MMM-yyyy HH:mm:ss", maConfig.getOutputTimeFormat());
        assertEquals("site", maConfig.getOutputGroupName());
    }

    @Test
    public void testXmlEscaping() throws Exception {
        MAConfig maConfig1 = new MAConfig();
        maConfig1.setGoodPixelExpression("a < b");
        maConfig1.setGoodRecordExpression("c > d");

        String xml = maConfig1.toXml();
        assertTrue(xml.contains("<goodPixelExpression>a &lt; b</goodPixelExpression>"));
        assertTrue(xml.contains("<goodRecordExpression>c &gt; d</goodRecordExpression>"));

        MAConfig maConfig2 = MAConfig.fromXml(xml);
        assertEquals(maConfig1.getGoodPixelExpression(), maConfig2.getGoodPixelExpression());
        assertEquals(maConfig1.getGoodRecordExpression(), maConfig2.getGoodRecordExpression());
    }

    @Test
    public void testCreateRecordSource() throws Exception {
        MAConfig maConfig = new MAConfig();
        maConfig.setRecordSourceSpiClassName(TestRecordSourceSpi.class.getName());
        RecordSource recordSource = maConfig.createRecordSource(new Configuration());
        assertNotNull(recordSource);
        assertNotNull(recordSource.getRecords());
        Iterator<Record> recordIterator = recordSource.getRecords().iterator();
        assertTrue(recordIterator.hasNext());
        Record record = recordIterator.next();
        assertNotNull(record);
    }

    @Test
    public void testIsMaxTimeDifferenceValid() throws Exception {
        assertTrue(MAConfig.isMaxTimeDifferenceValid(null));
        assertTrue(MAConfig.isMaxTimeDifferenceValid(""));
        assertTrue(MAConfig.isMaxTimeDifferenceValid(" "));
        assertTrue(MAConfig.isMaxTimeDifferenceValid("0"));
        assertTrue(MAConfig.isMaxTimeDifferenceValid("0.0"));
        assertTrue(MAConfig.isMaxTimeDifferenceValid("1"));
        assertTrue(MAConfig.isMaxTimeDifferenceValid("1.2"));
        assertTrue(MAConfig.isMaxTimeDifferenceValid(" 1.2 "));
        assertTrue(MAConfig.isMaxTimeDifferenceValid("-0"));
        assertTrue(MAConfig.isMaxTimeDifferenceValid("0d"));
        assertTrue(MAConfig.isMaxTimeDifferenceValid("1d"));
        assertTrue(MAConfig.isMaxTimeDifferenceValid("2d"));
        assertFalse(MAConfig.isMaxTimeDifferenceValid("0.0d"));
        assertFalse(MAConfig.isMaxTimeDifferenceValid("-1d"));
        assertFalse(MAConfig.isMaxTimeDifferenceValid("-1"));
        assertFalse(MAConfig.isMaxTimeDifferenceValid("-2"));
        assertFalse(MAConfig.isMaxTimeDifferenceValid("1w"));
    }
}
