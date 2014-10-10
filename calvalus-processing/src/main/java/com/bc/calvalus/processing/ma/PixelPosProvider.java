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

import java.awt.Rectangle;
import java.awt.geom.Area;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

/**
 * Provides a {@code PixelPos} for a given {@code Record}, if possible.
 */
public class PixelPosProvider {

    private final Product product;
    private final PixelTimeProvider pixelTimeProvider;
    private final long maxTimeDifference; // Note: time in ms (NOT h)
    private final long productStartTime;
    private final long productEndTime;
    // todo make this a parameter
    private final int allowedPixelDisplacement;

    private List<PixelPosProvider.PixelPosRecord> pixelPosRecords;
    private Area pixelArea;


    public PixelPosProvider(Product product, PixelTimeProvider pixelTimeProvider, Double maxTimeDifference,
                            boolean hasReferenceTime) {
        this.product = product;
        this.pixelTimeProvider = pixelTimeProvider;

        if (maxTimeDifference != null && hasReferenceTime) {
            this.maxTimeDifference = Math.round(maxTimeDifference * 60 * 60 * 1000); // h to ms
        } else {
            this.maxTimeDifference = 0L;
        }
        if (testTime()) {
            long endTime = product.getEndTime().getAsDate().getTime();
            long startTime = product.getStartTime().getAsDate().getTime();
            if (startTime <= endTime) {
                productStartTime = startTime;
                productEndTime = endTime;
            } else {
                productStartTime = startTime;
                productEndTime = endTime;
            }
        } else {
            productStartTime = productEndTime = 0L;
        }
        allowedPixelDisplacement = 5;
    }

    /**
     * Gets the temporally and spatially valid pixel position.
     *
     * @param referenceRecord The reference record
     * @return The pixel position, or {@code null} if no such exist.
     */
    public PixelPos getPixelPos(Record referenceRecord) {
        return getTemporallyAndSpatiallyValidPixelPos(referenceRecord);
    }

    public PixelPos getTemporallyAndSpatiallyValidPixelPos(Record referenceRecord) {

        if (testTime()) {

            long minReferenceTime = getMinReferenceTime(referenceRecord);
            if (minReferenceTime > productEndTime) {
                return null;
            }

            long maxReferenceTime = getMaxReferenceTime(referenceRecord);
            if (maxReferenceTime < productStartTime) {
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

    private List<PixelPosRecord> getInputRecordsSortedByPixelYX(Iterable<Record> inputRecords) {
        ArrayList<PixelPosRecord> pixelPosList = new ArrayList<>(128);
        for (Record inputRecord : inputRecords) {
            final PixelPos pixelPos = getPixelPos(inputRecord);
            if (pixelPos != null) {
                pixelPosList.add(new PixelPosRecord(pixelPos, inputRecord));
            }
        }
        PixelPosRecord[] records = pixelPosList.toArray(new PixelPosRecord[pixelPosList.size()]);
        Arrays.sort(records, new YXComparator());
        return Arrays.asList(records);
    }

    public void computePixelPosRecords(Iterable<Record> referenceRecords, int macroPixelSize) {
        pixelPosRecords = getInputRecordsSortedByPixelYX(referenceRecords);
        pixelArea = new Area();

        for (PixelPosProvider.PixelPosRecord pixelPosRecord : pixelPosRecords) {
            PixelPos pixelPos = pixelPosRecord.getPixelPos();
            Rectangle rectangle = new Rectangle((int) pixelPos.x - macroPixelSize / 2,
                                                (int) pixelPos.y - macroPixelSize / 2,
                                                macroPixelSize, macroPixelSize);
            pixelArea.add(new Area(rectangle));
        }
    }

    public List<PixelPosRecord> getPixelPosRecords() {
        return pixelPosRecords;
    }

    public Area getPixelArea() {
        return pixelArea;
    }

    static class YXComparator implements Comparator<PixelPosRecord> {

        @Override
        public int compare(PixelPosRecord o1, PixelPosRecord o2) {
            // because in the code we get the pixel-pos by the geo-pos it can happen that pixels on the same line are
            // wrongly sorted because of the little difference in y-direction.
            // that's the reason why we compare y-position only as integer
            int y1 = (int) o1.pixelPos.y;
            int y2 = (int) o2.pixelPos.y;
            if (y1 < y2) {
                return -2;
            } else if (y1 > y2) {
                return 2;
            }
            float x1 = o1.pixelPos.x;
            float x2 = o2.pixelPos.x;
            if (x1 < x2) {
                return -1;
            } else if (x1 > x2) {
                return 1;
            }
            return 0;
        }
    }

    static class PixelPosRecordFactory {

        private final int xAttributeIndex;
        private final int yAttributeIndex;
        private final int timeAttributeIndex;

        PixelPosRecordFactory(Header header) {
            xAttributeIndex = header.getAttributeIndex(PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX + ProductRecordSource.PIXEL_X_ATT_NAME);
            yAttributeIndex = header.getAttributeIndex(PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX + ProductRecordSource.PIXEL_Y_ATT_NAME);
            timeAttributeIndex = header.getAttributeIndex(ProductRecordSource.PIXEL_TIME_ATT_NAME);
        }

        PixelPosRecord create(Record record) {
            Object[] attributeValues = record.getAttributeValues();
            int xPos = 0;
            int yPos = 0;
            if (attributeValues[xAttributeIndex] instanceof AggregatedNumber &&
                attributeValues[yAttributeIndex] instanceof AggregatedNumber) {
                float[] xAttributeValue = ((AggregatedNumber) attributeValues[xAttributeIndex]).data;
                float[] yAttributeValue = ((AggregatedNumber) attributeValues[yAttributeIndex]).data;
                xPos = (int) xAttributeValue[xAttributeValue.length / 2];
                yPos = (int) yAttributeValue[yAttributeValue.length / 2];
            } else if (attributeValues[xAttributeIndex] instanceof Integer &&
                       attributeValues[yAttributeIndex] instanceof Integer) {
                xPos = (Integer) attributeValues[xAttributeIndex];
                yPos = (Integer) attributeValues[yAttributeIndex];
            }
            // can be null !!!!
            Date eoTime = (Date) attributeValues[timeAttributeIndex];
            Date insituTime = record.getTime();
            long timeDiff = -1;
            if (insituTime != null && eoTime != null) {
                timeDiff = Math.abs(insituTime.getTime() - eoTime.getTime());
            }
            return new PixelPosRecord(new PixelPos(xPos, yPos), record, timeDiff);
        }
    }

    public static class PixelPosRecord {

        private final PixelPos pixelPos;
        private final Record record;
        private final long timeDiff;

        PixelPosRecord(PixelPos pixelPos, Record record) {
            this(pixelPos, record, -1);
        }

        PixelPosRecord(PixelPos pixelPos, Record record, long timeDiff) {
            this.record = record;
            this.pixelPos = pixelPos;
            this.timeDiff = timeDiff;
        }

        public PixelPos getPixelPos() {
            return pixelPos;
        }

        public long getTimeDifference() {
            return timeDiff;
        }

        public Record getRecord() {
            return record;
        }

        @Override
        public String toString() {
            return "PixelPosRecord{" +
                   "pixelPos=" + pixelPos +
                   ", record=" + record +
                   ", timeDiff=" + timeDiff +
                   '}';
        }
    }
}
