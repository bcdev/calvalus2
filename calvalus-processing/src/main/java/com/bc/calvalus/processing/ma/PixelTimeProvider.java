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

import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

import java.util.Date;

/**
* Provides a time for each pixel position.
*/
public class PixelTimeProvider {

    private final double startMJD;
    private final double deltaMjdPerLine;

    public static PixelTimeProvider create(Product product) {
        ProductData.UTC startTime = product.getStartTime();
        ProductData.UTC endTime = product.getEndTime();
        int rasterHeight = product.getSceneRasterHeight();
        return create(startTime, endTime, rasterHeight);
    }
    
    public static PixelTimeProvider create(ProductData.UTC startTime, ProductData.UTC endTime, int rasterHeight) {
        if (startTime != null && endTime != null && rasterHeight > 1) {
            double deltaMJD = (endTime.getMJD() - startTime.getMJD()) / (rasterHeight - 1);
            return new PixelTimeProvider(startTime.getMJD(), deltaMJD);
        } else {
            return null;
        }
    }

    private PixelTimeProvider(double startMJD, double deltaMJD) {
        this.startMJD = startMJD;
        this.deltaMjdPerLine = deltaMJD;
    }

    public Date getTime(PixelPos pixelPos) {
        return getUTC(pixelPos).getAsDate();
    }

    private ProductData.UTC getUTC(PixelPos pixelPos) {
        return new ProductData.UTC(startMJD + Math.floor(pixelPos.y) * deltaMjdPerLine);
    }

    @Override
    public String toString() {
        return "PixelTimeProvider{" +
                "startMJD=" + new ProductData.UTC(startMJD).format() +
                ", deltaMjdPerLine=" + deltaMjdPerLine +
                '}';
    }
}
