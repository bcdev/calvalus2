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


import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MosaicFormatterTest {

    @Test
    public void testGetPartitionNumber() throws Exception {
        assertEquals(0, MosaicFormatter.getPartitionNumber("part-r-00000"));
        assertEquals(1, MosaicFormatter.getPartitionNumber("part-r-00001"));
        assertEquals(15, MosaicFormatter.getPartitionNumber("part-r-00015"));
    }

    @Test
    public void testGetPartGeometry() throws Exception {
        Geometry partGeometry = MosaicFormatter.getPartGeometry(0, 18);
        assertNotNull(partGeometry);
        assertTrue(partGeometry.isRectangle());
        Envelope envelope = partGeometry.getEnvelopeInternal();
        assertEquals("minX", -180.0, envelope.getMinX(), 1e-5);
        assertEquals("maxX", +180.0, envelope.getMaxX(), 1e-5);
        assertEquals("minY", 80.0, envelope.getMinY(), 1e-5);
        assertEquals("maxY", 90.0, envelope.getMaxY(), 1e-5);

        partGeometry = MosaicFormatter.getPartGeometry(9, 18);
        assertNotNull(partGeometry);
        assertTrue(partGeometry.isRectangle());
        envelope = partGeometry.getEnvelopeInternal();
        assertEquals("minX", -180.0, envelope.getMinX(), 1e-5);
        assertEquals("maxX", +180.0, envelope.getMaxX(), 1e-5);
        assertEquals("minY", -10.0, envelope.getMinY(), 1e-5);
        assertEquals("maxY", 0.0, envelope.getMaxY(), 1e-5);

        partGeometry = MosaicFormatter.getPartGeometry(17, 18);
        assertNotNull(partGeometry);
        assertTrue(partGeometry.isRectangle());
        envelope = partGeometry.getEnvelopeInternal();
        assertEquals("minX", -180.0, envelope.getMinX(), 1e-5);
        assertEquals("maxX", +180.0, envelope.getMaxX(), 1e-5);
        assertEquals("minY", -90.0, envelope.getMinY(), 1e-5);
        assertEquals("maxY", -80.0, envelope.getMaxY(), 1e-5);
    }
}
