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

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

public class GeometryUtils {

    public static Geometry createGeometry(String geometryWkt) {
        Geometry geometry = parseWKT(geometryWkt);
        if (geometry != null && !isGlobalCoverageGeometry(geometry)) {
            int unwrapDateline = DateLineOps.unwrapDateline(geometry, -180, 180);
            if (unwrapDateline > 0) {
                geometry = DateLineOps.pageGeom(geometry, -180, 180);
            }
        }
        return geometry;
    }

    public static boolean isGlobalCoverageGeometry(Geometry geometry) {
        Envelope envelopeInternal = geometry.getEnvelopeInternal();
        return eq(envelopeInternal.getMinX(), -180.0, 1E-8)
               && eq(envelopeInternal.getMaxX(), 180.0, 1E-8)
               && eq(envelopeInternal.getMinY(), -90.0, 1E-8)
               && eq(envelopeInternal.getMaxY(), 90.0, 1E-8);
    }

    public static Geometry parseWKT(String geometryWkt) {
        if (geometryWkt == null || geometryWkt.isEmpty()) {
            return null;
        }
        final WKTReader wktReader = new WKTReader();
        try {
            return wktReader.read(geometryWkt);
        } catch (org.locationtech.jts.io.ParseException e) {
            throw new IllegalArgumentException("Illegal region geometry: " + geometryWkt, e);
        }
    }

    private static boolean eq(double x1, double x2, double eps) {
        double delta = x1 - x2;
        return delta > 0 ? delta < eps : -delta < eps;
    }

}
