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

package com.bc.calvalus.processing;

import com.vividsolutions.jts.geom.Geometry;
import org.esa.beam.framework.datamodel.Product;
import org.junit.Test;

import java.io.IOException;

import static com.bc.calvalus.processing.JobUtils.createGeometry;
import static com.bc.calvalus.processing.ProcessingRectangleCalculator.isGlobalCoverageGeometry;
import static org.junit.Assert.*;

/**
 * @author Norman
 */
public class ProcessingRectangleCalculatorTest {

    @Test
    public void testIsGlobeCoverageGeometry() throws Exception {
        assertTrue(isGlobalCoverageGeometry(createGeometry("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))")));
        assertTrue(isGlobalCoverageGeometry(createGeometry("POLYGON((-180 -90, -180 90, 180 90,  180 -90,  -180 -90))")));
        assertFalse(isGlobalCoverageGeometry(createGeometry("POLYGON((-180 -90, 180 -90, 180 89, -180 89, -180 -90))")));
        assertFalse(isGlobalCoverageGeometry(createGeometry("POLYGON((-180 -90, 80 -90, 80 89, -180 89, -180 -90))")));
    }

    @Test
    public void testGetCombinedGeometry() throws Exception {
        Geometry combinedGeometry = getCalculcator(null, null).getCombinedGeometry();
        assertNull(combinedGeometry);
        combinedGeometry = getCalculcator("Polygon((10 10, 10 20, 20 20, 20 10, 10 10))", null).getCombinedGeometry();
        assertNotNull(combinedGeometry);
        assertEquals("POLYGON ((10 10, 10 20, 20 20, 20 10, 10 10))", combinedGeometry.toString());
        combinedGeometry = getCalculcator(null, "Polygon((10 10, 10 20, 20 20, 20 10, 10 10))").getCombinedGeometry();
        assertNotNull(combinedGeometry);
        assertEquals("POLYGON ((10 10, 10 20, 20 20, 20 10, 10 10))", combinedGeometry.toString());
        combinedGeometry = getCalculcator("Polygon((10 10, 10 20, 20 20, 20 10, 10 10))", "Polygon((10 10, 10 20, 20 20, 20 10, 10 10))").getCombinedGeometry();
        assertNotNull(combinedGeometry);
        assertEquals("POLYGON ((10 10, 10 20, 20 20, 20 10, 10 10))", combinedGeometry.toString());
        combinedGeometry = getCalculcator("Polygon((10 15, 10 20, 20 20, 20 15, 10 15))", "Polygon((15 10, 15 20, 20 20, 20 10, 15 10))").getCombinedGeometry();
        assertNotNull(combinedGeometry);
        assertEquals("POLYGON ((15 20, 20 20, 20 15, 15 15, 15 20))", combinedGeometry.toString());

    }

    private ProcessingRectangleCalculator getCalculcator(String wkt1, String wkt2) {
        Geometry geometry1 = JobUtils.createGeometry(wkt1);
        Geometry geometry2 = JobUtils.createGeometry(wkt2);
        return new ProcessingRectangleCalculator(geometry1, geometry2, null) {
                @Override
                Product getProduct() throws IOException {
                    return null;
                }
            };
    }
}
