/*
 * Copyright (C) 2015 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.utils;

import org.locationtech.jts.geom.Geometry;
import org.junit.Test;

import static com.bc.calvalus.processing.utils.GeometryUtils.isGlobalCoverageGeometry;
import static com.bc.calvalus.processing.utils.GeometryUtils.parseWKT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GeometryUtilsTest {

    @Test
    public void testIsGlobeCoverageGeometry() throws Exception {
        assertTrue(isGlobalCoverageGeometry(parseWKT("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))")));
        assertTrue(isGlobalCoverageGeometry(parseWKT("POLYGON((-180 -90, -180 90, 180 90,  180 -90,  -180 -90))")));
        assertFalse(isGlobalCoverageGeometry(parseWKT("POLYGON((-180 -90, 180 -90, 180 89, -180 89, -180 -90))")));
        assertFalse(isGlobalCoverageGeometry(parseWKT("POLYGON((-180 -90, 80 -90, 80 89, -180 89, -180 -90))")));
    }

    @Test
    public void testDateLineUnwrapping() throws Exception {
        String fiji = "POLYGON ((179.7 -15.9, 178.4 -16.4, 177.4 -16.6, 177.4 -16.8, 177.5 -17.1, 178.8 -17.1, -179.8 -16.5, 179.7 -15.9, 179.7 -15.9))";
        Geometry geometry = GeometryUtils.createGeometry(fiji);
        assertNotNull(geometry);
        String fijiUnWrapped = "MULTIPOLYGON (((180 -16.260000000000012, 180 -16.585714285714282, 178.8 -17.1, 177.5 -17.1, 177.4 -16.8, 177.4 -16.6, 178.4 -16.4, 179.7 -15.9, 180 -16.260000000000012)), ((-180 -16.585714285714282, -180 -16.260000000000012, -179.8 -16.5, -180 -16.585714285714282)))";
        assertEquals(fijiUnWrapped, geometry.toText());
    }
}