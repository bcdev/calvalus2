/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.ra;

import com.bc.calvalus.processing.utils.GeometryUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.union.CascadedPolygonUnion;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

public class JtsTest {

    private static final String NORTH_SEA_WKT = "polygon((-19.94 40.00, 0.00 40.00, 0.00 49.22, 12.99 53.99, 13.06 65.00, 0.00 65.00, 0.0 60.00, -20.00 60.00, -19.94 40.00))";
    private static final String BALTIC_SEA_WKT = "polygon((10.00 54.00,  14.27 53.47,  20.00 54.00, 21.68 54.77, 22.00 56.70, 24.84 56.70, 30.86 60.01, 26.00 62.00, 26.00 66.00, 22.00 66.00, 10.00 60.00, 10.00 54.00))";
    private static final String ACADIA_WKT = "polygon((-71.00 41.00, -52.00 41.00, -52.00 52.00, -71.00 52.00, -71.00 41.00))";

    @Test
    public void testJTS() throws Exception {
        final Geometry poly1= GeometryUtils.createGeometry(NORTH_SEA_WKT);
        final Geometry poly2= GeometryUtils.createGeometry(ACADIA_WKT);

        Geometry union = poly1.union(poly2);
        System.out.println("union: ");
        System.out.println(union.getEnvelope());
        System.out.println();

        Geometry cascadedUnion = CascadedPolygonUnion.union(Arrays.asList(poly1, poly2));
        System.out.println("cascadedUnion: ");
        System.out.println(cascadedUnion);
        System.out.println(cascadedUnion.getBoundary());
        System.out.println(cascadedUnion.getEnvelope());
        System.out.println();

    }
}
