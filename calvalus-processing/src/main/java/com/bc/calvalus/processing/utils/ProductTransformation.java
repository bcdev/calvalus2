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

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProductTransformation {

    private final AffineTransform transform;

    public ProductTransformation(Rectangle subsetRectangle, boolean flipX, boolean flipY) {
        transform = subset(subsetRectangle);
        if (flipY) {
            transform.preConcatenate(flipY(subsetRectangle));
        }
        if (flipX) {
            transform.preConcatenate(flipX(subsetRectangle));
        }
    }

    public static ProductTransformation parse(String productTransformationString, Rectangle productRectangle) {
        String input = productTransformationString.toLowerCase();

        if (input.contains("identity")) {
            return new ProductTransformation(productRectangle, false, false);
        }

        Rectangle subsetRect = parseSubset(input, productRectangle);
        boolean flipX = parseFlip(input, "x");
        boolean flipY = parseFlip(input, "y");

        return new ProductTransformation(subsetRect, flipX, flipY);
    }

    static boolean parseFlip(String input, String letter) {
        return input.contains("flip" + letter);
    }

    static Rectangle parseSubset(String input, Rectangle productRectangle) {
        Pattern p = Pattern.compile("subset\\s+(\\d+)\\s*,\\s*(\\d+)\\s*,\\s*(\\d+)\\s*,\\s*(\\d+).*");
        Matcher m = p.matcher(input);
        if (m.matches()) {
            String xString = m.group(1);
            String yString = m.group(2);
            String wString = m.group(3);
            String hString = m.group(4);
            int x = Integer.parseInt(xString);
            int y = Integer.parseInt(yString);
            int w = Integer.parseInt(wString);
            int h = Integer.parseInt(hString);
            return new Rectangle(x, y, w ,h);
        }
        return productRectangle;
    }

    public AffineTransform getTransform() {
        return transform;
    }

    static AffineTransform subset(Rectangle subsetProduct) {
        AffineTransform i2o = new AffineTransform();
        i2o.translate(-subsetProduct.getX(), -subsetProduct.getY());
        return i2o;
    }

    static AffineTransform flipY(Rectangle subsetProduct) {
        AffineTransform i2o = new AffineTransform();
        i2o.scale(-1, 1);
        i2o.translate(-subsetProduct.getWidth(), 0);
        return i2o;
    }

    static AffineTransform flipX(Rectangle subsetProduct) {
        AffineTransform i2o = new AffineTransform();
        i2o.scale(1, -1);
        i2o.translate(0, -subsetProduct.getHeight());
        return i2o;
    }
}
