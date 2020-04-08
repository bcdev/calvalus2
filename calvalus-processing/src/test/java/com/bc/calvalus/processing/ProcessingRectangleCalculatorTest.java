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

import com.bc.calvalus.processing.utils.GeometryUtils;
import org.locationtech.jts.geom.Geometry;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.junit.Ignore;
import org.junit.Test;

import java.awt.Rectangle;
import java.io.IOException;

import static com.bc.calvalus.processing.ProcessingRectangleCalculator.intersectionSafe;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/**
 * @author Norman
 */
public class ProcessingRectangleCalculatorTest {

    @Test
    public void testIntersectionSafe() throws Exception {
        Rectangle r1 = new Rectangle(30, 30);
        Rectangle r2 = new Rectangle(10, 10, 30, 30);
        Rectangle r3 = new Rectangle(10, 10, 20, 20);
        assertNull(intersectionSafe(null, null));
        assertSame(r1, intersectionSafe(r1, null));
        assertSame(r2, intersectionSafe(null, r2));
        assertEquals(r3, intersectionSafe(r1, r2));

    }

    @Ignore
    @Test
    public void getGeometryAsRectangle() throws IOException {
        System.setProperty("snap.pixelGeoCoding.useTiling", "true");
        System.setProperty("snap.useAlternatePixelGeoCoding", "true");

        Product product = ProductIO.readProduct("C:\\ssd\\fire\\S3A_OL_1_EFR____20190101T144647_20190101T144947_20190102T215705_0179_039_367_2880_MAR_O_NT_002.tif_idepix.nc");
        String geometryWkt = "POLYGON((-80 10, -70 10, -70 0, -80 0, -80 10))";
        Geometry regionGeometry = GeometryUtils.createGeometry(geometryWkt);
//        Rectangle rectangle = ProcessingRectangleCalculator.getGeometryAsRectangle(product, regionGeometry);
        System.out.println(product.getSceneGeoCoding());
//        System.out.println(rectangle);

        System.setProperty("snap.pixelGeoCoding.useTiling", "true");
        System.setProperty("snap.useAlternatePixelGeoCoding", "false");

//        product = ProductIO.readProduct("C:\\ssd\\fire\\S3A_OL_1_EFR____20190101T144647_20190101T144947_20190102T215705_0179_039_367_2880_MAR_O_NT_002.tif_idepix.nc");
//        rectangle = ProcessingRectangleCalculator.getGeometryAsRectangle(product, regionGeometry);
//        System.out.println(product.getSceneGeoCoding());
//        System.out.println(rectangle);

        System.setProperty("snap.pixelGeoCoding.useTiling", "false");
        System.setProperty("snap.useAlternatePixelGeoCoding", "true");

        product = ProductIO.readProduct("C:\\ssd\\fire\\S3A_OL_1_EFR____20190101T144647_20190101T144947_20190102T215705_0179_039_367_2880_MAR_O_NT_002.tif_idepix.nc");
        Rectangle rectangle = ProcessingRectangleCalculator.getGeometryAsRectangle(product, regionGeometry);
        System.out.println(product.getSceneGeoCoding());
        System.out.println(rectangle);
    }

}
