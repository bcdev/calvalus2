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

package com.bc.calvalus.processing.utils;

import org.junit.Test;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.awt.geom.Point2D;

import static org.junit.Assert.*;

public class ProductTransformationTest {

    private static final Rectangle INPUT_PRODUCT = new Rectangle(100, 500);
    private static final Rectangle SUBSET_PRODUCT = new Rectangle(10, 20, 30, 30);

    @Test
    public void testTransformationSubset() throws Exception {
        AffineTransform i2o = new ProductTransformation(SUBSET_PRODUCT, false, false).getTransform();

        // in
        assertEquals(point(5f, 5f), i2o.transform(point(15f, 25f), null));
        // corners
        assertEquals(point(0f, 0f), i2o.transform(point(10f, 20f), null));
        assertEquals(point(30f, 0f), i2o.transform(point(40f, 20f), null));
        assertEquals(point(30f, 30f), i2o.transform(point(40f, 50f), null));
        assertEquals(point(0f, 30f), i2o.transform(point(10f, 50f), null));
    }

    @Test
    public void testTransformationFlipY() throws Exception {
        AffineTransform i2o = new ProductTransformation(INPUT_PRODUCT, false, true).getTransform();

        // in
        assertEquals(point(85f, 25f), i2o.transform(point(15f, 25f), null));
        // corners
        assertEquals(point(90f, 20f), i2o.transform(point(10f, 20f), null));
        assertEquals(point(60f, 20f), i2o.transform(point(40f, 20f), null));
        assertEquals(point(60f, 50f), i2o.transform(point(40f, 50f), null));
        assertEquals(point(90f, 50f), i2o.transform(point(10f, 50f), null));
    }

    @Test
    public void testTransformationFlipX() throws Exception {
        AffineTransform i2o = new ProductTransformation(INPUT_PRODUCT, true, false).getTransform();

        // in
        assertEquals(point(15f, 475f), i2o.transform(point(15f, 25f), null));
        // corners
        assertEquals(point(10f, 480f), i2o.transform(point(10f, 20f), null));
        assertEquals(point(40f, 480f), i2o.transform(point(40f, 20f), null));
        assertEquals(point(40f, 450f), i2o.transform(point(40f, 50f), null));
        assertEquals(point(10f, 450f), i2o.transform(point(10f, 50f), null));
    }


    @Test
    public void testTransformationSubsetAndFlipY() throws Exception {
        AffineTransform i2o = new ProductTransformation(SUBSET_PRODUCT, false, true).getTransform();

        // in
        assertEquals(point(25f, 5f), i2o.transform(point(15f, 25f), null));
        // corners
        assertEquals(point(30f, 0f), i2o.transform(point(10f, 20f), null));
        assertEquals(point(0f, 0f), i2o.transform(point(40f, 20f), null));
        assertEquals(point(0f, 30f), i2o.transform(point(40f, 50f), null));
        assertEquals(point(30f, 30f), i2o.transform(point(10f, 50f), null));
    }

    @Test
    public void testTransformationSubsetAndFlipX() throws Exception {
        AffineTransform i2o = new ProductTransformation(SUBSET_PRODUCT, true, false).getTransform();

        // in
        assertEquals(point(5f, 25f), i2o.transform(point(15f, 25f), null));
        // corners
        assertEquals(point(0f, 30f), i2o.transform(point(10f, 20f), null));
        assertEquals(point(30f, 30f), i2o.transform(point(40f, 20f), null));
        assertEquals(point(30f, 0f), i2o.transform(point(40f, 50f), null));
        assertEquals(point(0f, 0f), i2o.transform(point(10f, 50f), null));
    }

    @Test
    public void testTransformationSubsetAndFlipYAndFlipX() throws Exception {
        AffineTransform i2o = new ProductTransformation(SUBSET_PRODUCT, true, true).getTransform();

        // in
        assertEquals(point(25f, 25f), i2o.transform(point(15f, 25f), null));
        // corners
        assertEquals(point(30f, 30f), i2o.transform(point(10f, 20f), null));
        assertEquals(point(0f, 30f), i2o.transform(point(40f, 20f), null));
        assertEquals(point(0f, 0f), i2o.transform(point(40f, 50f), null));
        assertEquals(point(30f, 0f), i2o.transform(point(10f, 50f), null));
    }

    private static Point2D.Float point(float x, float y) {
        return new Point2D.Float(x, y);
    }

    @Test
    public void testParseSubset() throws Exception {
        Rectangle productRectangle = new Rectangle(30, 40);

        Rectangle subsetRect = ProductTransformation.parseSubset("subset 2,3,4,5 flipX flipY", productRectangle);
        assertEquals(new Rectangle(2, 3, 4, 5), subsetRect);

        subsetRect = ProductTransformation.parseSubset("subset 21, 32   , 46 , 567", productRectangle);
        assertEquals(new Rectangle(21, 32, 46, 567), subsetRect);

        subsetRect = ProductTransformation.parseSubset("subset 21, 32   ", productRectangle);
        assertEquals(productRectangle, subsetRect);
    }

    @Test
    public void testParseFlip() throws Exception {
        assertTrue(ProductTransformation.parseFlip("foo bar flipx", "x"));
        assertTrue(ProductTransformation.parseFlip("foo flipy bar", "y"));
        assertFalse(ProductTransformation.parseFlip("foo bar", "x"));
        assertFalse(ProductTransformation.parseFlip("foo bar flipy", "x"));
    }

    // polymer MERIS:   subset, flipY
    // polymer MODIS:   subset, flipY, flipX
    // polymer SeaWiFS: subset, flipY, flipX

    // l2gen MERIS   ok
    // l2gen MODIS   ok
    // l2gen SeaWiFS ok


}