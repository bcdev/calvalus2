/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import org.esa.beam.framework.datamodel.GeoPos;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.*;

public class ProductOffsetTransformerTest {

    @Test
    public void testWithoutOffsets() throws Exception {
        Record transformedRecord = new ProductOffsetTransformer(4, 5, 0, 0).transform(
                createDataRecord(new int[]{64, 32, 16, 8},
                                 new int[]{64, 32, 16, 8},
                                 new float[]{1.1F, 1.2F, Float.NaN, 1.8F}));
        assertNotNull(transformedRecord);
        Object[] attributeValues = transformedRecord.getAttributeValues();
        assertEquals(7, attributeValues.length);

        assertSame(String.class, attributeValues[3].getClass());
        String label = (String) attributeValues[3];
        assertEquals("africa", label);

        int[] xValues = (int[]) attributeValues[4];
        assertEquals(4, xValues.length);
        assertArrayEquals(new int[]{64, 32, 16, 8}, xValues);

        int[] yValues = (int[]) attributeValues[5];
        assertEquals(4, yValues.length);
        assertArrayEquals(new int[]{64, 32, 16, 8}, yValues);
    }

    @Test
    public void testWithOffsets() throws Exception {
        Record transformedRecord = new ProductOffsetTransformer(4, 5, 3, 5).transform(
                createDataRecord(new int[]{64, 32, 16, 8},
                                 new int[]{64, 32, 16, 8},
                                 new float[]{1.1F, 1.2F, Float.NaN, 1.8F}));
        assertNotNull(transformedRecord);
        Object[] attributeValues = transformedRecord.getAttributeValues();
        assertEquals(7, attributeValues.length);

        assertSame(String.class, attributeValues[3].getClass());
        String label = (String) attributeValues[3];
        assertEquals("africa", label);

        int[] xValues = (int[]) attributeValues[4];
        assertEquals(4, xValues.length);
        assertArrayEquals(new int[]{67, 35, 19, 11}, xValues);

        int[] yValues = (int[]) attributeValues[5];
        assertEquals(4, yValues.length);
        assertArrayEquals(new int[]{69, 37, 21, 13}, yValues);
    }

    private Record createDataRecord(int[] pixelX, int[] pixelY, float[] measurements) {
        return RecordUtils.create(new GeoPos(53.0F, 13.3F), new Date(128L),
                                  "africa",
                                  pixelX,
                                  pixelY,
                                  measurements);
    }

}
