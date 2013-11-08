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

import org.esa.beam.framework.datamodel.GeoCoding;
import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.PixelPos;
import org.esa.beam.framework.datamodel.Product;

import java.util.Date;

/**
 * Provides a {@code PixelPos} for a given {@code Record}, if possible.
 */
class PixelPosProvider {

    private final Product product;
    private final PixelTimeProvider pixelTimeProvider;
    private final long maxTimeDifference; // Note: time in ms (NOT h)
    // todo make this a parameter
    private int allowedPixelDisplacement;


    public PixelPosProvider(Product product, PixelTimeProvider pixelTimeProvider, Double maxTimeDifference,
                            boolean hasReferenceTime) {
        this.product = product;
        this.pixelTimeProvider = pixelTimeProvider;

        if (maxTimeDifference != null && hasReferenceTime) {
            this.maxTimeDifference = Math.round(maxTimeDifference * 60 * 60 * 1000); // h to ms
        } else {
            this.maxTimeDifference = 0L;
        }
        allowedPixelDisplacement = 5;
    }

    /**
     * Gets the temporally and spatially valid pixel position.
     *
     * @param referenceRecord The reference record
     *
     * @return The pixel position, or {@code null} if no such exist.
     */
    public PixelPos getPixelPos(Record referenceRecord) {
        return getTemporallyAndSpatiallyValidPixelPos(referenceRecord);
    }

    public PixelPos getTemporallyAndSpatiallyValidPixelPos(Record referenceRecord) {

        if (testTime()) {

            long minReferenceTime = getMinReferenceTime(referenceRecord);
            if (minReferenceTime > product.getEndTime().getAsDate().getTime()) {
                return null;
            }

            long maxReferenceTime = getMaxReferenceTime(referenceRecord);
            if (maxReferenceTime < product.getStartTime().getAsDate().getTime()) {
                return null;
            }

            PixelPos pixelPos = getSpatiallyValidPixelPos(referenceRecord);
            if (pixelPos != null) {
                long pixelTime = pixelTimeProvider.getTime(pixelPos).getTime();
                if (pixelTime >= minReferenceTime && pixelTime <= maxReferenceTime) {
                    return pixelPos;
                }
            }
        } else {
            PixelPos pixelPos = getSpatiallyValidPixelPos(referenceRecord);
            if (pixelPos != null) {
                return pixelPos;
            }
        }
        return null;
    }

    private boolean testTime() {
        return maxTimeDifference > 0 && pixelTimeProvider != null;
    }

    private PixelPos getSpatiallyValidPixelPos(Record referenceRecord) {
        GeoPos location = referenceRecord.getLocation();
        if (location == null) {
            return null;
        }
        GeoCoding geoCoding = product.getGeoCoding();
        final PixelPos pixelPos = geoCoding.getPixelPos(location, null);
        if (pixelPos.isValid() && product.containsPixel(pixelPos)) {
            if (allowedPixelDisplacement < 0) {
                return pixelPos;
            }
            GeoPos geoPos = geoCoding.getGeoPos(pixelPos, null);
            PixelPos pixelPos2 = geoCoding.getPixelPos(geoPos, null);
            float dx = pixelPos.x - pixelPos2.x;
            float dy = pixelPos.y - pixelPos2.y;
            if (Math.max(Math.abs(dx), Math.abs(dy)) < allowedPixelDisplacement) {
                return pixelPos;
            }
        }
        return null;
    }

    private long getMinReferenceTime(Record referenceRecord) {
        Date time = referenceRecord.getTime();
        if (time == null) {
            throw new IllegalArgumentException("Point record has no time information.");
        }
        return time.getTime() - maxTimeDifference;
    }

    private long getMaxReferenceTime(Record referenceRecord) {
        Date time = referenceRecord.getTime();
        if (time == null) {
            throw new IllegalArgumentException("Point record has no time information.");
        }
        return time.getTime() + maxTimeDifference;
    }

}
