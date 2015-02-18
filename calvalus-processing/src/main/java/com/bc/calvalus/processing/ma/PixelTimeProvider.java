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

import org.esa.beam.framework.datamodel.PixelPos;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;

import java.util.Date;

/**
* Provides a time for each pixel position.
*/
public class PixelTimeProvider {

    private final double startMJD;
    private final double deltaMJD;

    public static PixelTimeProvider create(Product product) {
        final ProductData.UTC startTime = product.getStartTime();
        final ProductData.UTC endTime = product.getEndTime();
        final int rasterHeight = product.getSceneRasterHeight();
        if (startTime != null && endTime != null && rasterHeight > 1) {
            return new PixelTimeProvider(startTime.getMJD(),
                                         (endTime.getMJD() - startTime.getMJD()) / (rasterHeight - 1));
        } else {
            return null;
        }
    }

    private PixelTimeProvider(double startMJD, double deltaMJD) {
        this.startMJD = startMJD;
        this.deltaMJD = deltaMJD;
    }

    public Date getTime(PixelPos pixelPos) {
        return getUTC(pixelPos).getAsDate();
    }

    private ProductData.UTC getUTC(PixelPos pixelPos) {
        return new ProductData.UTC(startMJD + Math.floor(pixelPos.y) * deltaMJD);
    }
}
