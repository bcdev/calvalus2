/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.mosaic;


import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MosaicFormatterMapperTest {

    @Test
    public void testGetPartitionNumber() throws Exception {
        assertEquals(0, MosaicFormatterMapper.getPartitionNumber("part-r-00000"));
        assertEquals(1, MosaicFormatterMapper.getPartitionNumber("part-r-00001"));
        assertEquals(15, MosaicFormatterMapper.getPartitionNumber("part-r-00015"));
    }

    @Test
    public void testGetPartGeometry() throws Exception {
        Geometry partGeometry = MosaicFormatterMapper.getPartGeometry(0, 18);
        assertNotNull(partGeometry);
        assertTrue(partGeometry.isRectangle());
        Envelope envelope = partGeometry.getEnvelopeInternal();
        assertEquals("minX", -180.0, envelope.getMinX(), 1e-5);
        assertEquals("maxX", +180.0, envelope.getMaxX(), 1e-5);
        assertEquals("minY", 80.0, envelope.getMinY(), 1e-5);
        assertEquals("maxY", 90.0, envelope.getMaxY(), 1e-5);

        partGeometry = MosaicFormatterMapper.getPartGeometry(9, 18);
        assertNotNull(partGeometry);
        assertTrue(partGeometry.isRectangle());
        envelope = partGeometry.getEnvelopeInternal();
        assertEquals("minX", -180.0, envelope.getMinX(), 1e-5);
        assertEquals("maxX", +180.0, envelope.getMaxX(), 1e-5);
        assertEquals("minY", -10.0, envelope.getMinY(), 1e-5);
        assertEquals("maxY", 0.0, envelope.getMaxY(), 1e-5);

        partGeometry = MosaicFormatterMapper.getPartGeometry(17, 18);
        assertNotNull(partGeometry);
        assertTrue(partGeometry.isRectangle());
        envelope = partGeometry.getEnvelopeInternal();
        assertEquals("minX", -180.0, envelope.getMinX(), 1e-5);
        assertEquals("maxX", +180.0, envelope.getMaxX(), 1e-5);
        assertEquals("minY", -90.0, envelope.getMinY(), 1e-5);
        assertEquals("maxY", -80.0, envelope.getMaxY(), 1e-5);
    }
}
