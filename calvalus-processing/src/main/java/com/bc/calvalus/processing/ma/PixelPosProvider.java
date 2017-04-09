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

import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import com.bc.calvalus.commons.CalvalusLogger;

import java.awt.Rectangle;
import java.awt.geom.Area;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;
import java.util.logging.Logger;

/**
 * Provides a {@code PixelPos} for a given {@code Record}, if possible.
 */
public class PixelPosProvider {
    private static final Logger LOG = CalvalusLogger.getLogger();

    private final Product product;
    private final PixelTimeProvider pixelTimeProvider;
    private final long maxTimeDifference; // Note: time in ms (NOT h)
    private final long productStartTime;
    private final long productEndTime;
    // todo make this a parameter
    private final int allowedPixelDisplacement;


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
     * Gets the temporally and spatially valid pixel position and time.
     * Wraps it in a pixel pos record
     *
     * @param referenceRecord The reference record
     * @return The pixel position, or {@code null} if no such exist.
     */
    private PixelPosRecord getPixelPosRecord(Record referenceRecord) {

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
                    return new PixelPosRecord(pixelPos, referenceRecord, pixelTime);
                }
            }
        } else {
            PixelPos pixelPos = getSpatiallyValidPixelPos(referenceRecord);
            if (pixelPos != null) {
                long pixelTime = -1;
                if (pixelTimeProvider != null) {
                    pixelTime = pixelTimeProvider.getTime(pixelPos).getTime();
                }
                return new PixelPosRecord(pixelPos, referenceRecord, pixelTime);
            }
        }
        return null;
    }

    private boolean testTime() {
        return maxTimeDifference != 0 && pixelTimeProvider != null;
    }

    private PixelPos getSpatiallyValidPixelPos(Record referenceRecord) {
        GeoPos location = referenceRecord.getLocation();
        if (location == null) {
            return null;
        }
        GeoCoding geoCoding = product.getSceneGeoCoding();
        final PixelPos pixelPos = geoCoding.getPixelPos(location, null);
        if (pixelPos.isValid() && product.containsPixel(pixelPos)) {
            if (allowedPixelDisplacement < 0) {
                return pixelPos;
            }
            GeoPos geoPos = geoCoding.getGeoPos(pixelPos, null);
            PixelPos pixelPos2 = geoCoding.getPixelPos(geoPos, null);
            double dx = pixelPos.x - pixelPos2.x;
            double dy = pixelPos.y - pixelPos2.y;
            if (Math.max(Math.abs(dx), Math.abs(dy)) < allowedPixelDisplacement) {
                return pixelPos;
            }
        }
        return null;
    }

    GregorianCalendar utc = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

    /** warning: not synchronized, must not be called concurrently */
    long getMinReferenceTime(Record referenceRecord) {
        Date time = referenceRecord.getTime();
        if (time == null) {
            throw new IllegalArgumentException("Point record has no time information.");
        }
        if (maxTimeDifference >= 0.0) {
            return time.getTime() - maxTimeDifference;
        } else {
            double lon = referenceRecord.getLocation().getLon();
            utc.setTimeInMillis(time.getTime() + (long) (lon / 15 * 60 * 60 * 1000));
            int hour = utc.get(Calendar.HOUR_OF_DAY);
            // screw to beginning of calendar day, or of previous day in case maxTD is -2 * ...
            return time.getTime() - hour * 60 * 60 * 1000 + 24 * (maxTimeDifference+60*60*1000);
        }
    }

    /** warning: not synchronized, must not be called concurrently */
    long getMaxReferenceTime(Record referenceRecord) {
        Date time = referenceRecord.getTime();
        if (time == null) {
            throw new IllegalArgumentException("Point record has no time information.");
        }
        if (maxTimeDifference >= 0.0) {
            return time.getTime() + maxTimeDifference;
        } else {
            double lon = referenceRecord.getLocation().getLon();
            utc.setTimeInMillis(time.getTime() + (long) (lon / 15 * 60 * 60 * 1000));
            int hour = utc.get(Calendar.HOUR_OF_DAY);
            // screw to beginning of next calendar day, or of day after in case maxTD is -2 * ...
            return time.getTime() + (24 - hour) * 60 * 60 * 1000 - 24 * (maxTimeDifference+60*60*1000);
        }
    }

    private List<PixelPosRecord> getInputRecordsSortedByPixelYX(Iterable<Record> inputRecords) {
        ArrayList<PixelPosRecord> pixelPosList = new ArrayList<>(128);
        for (Record inputRecord : inputRecords) {
            final PixelPosRecord pixelPosRecord = getPixelPosRecord(inputRecord);
            if (pixelPosRecord != null) {
                pixelPosList.add(pixelPosRecord);
            }
        }
        PixelPosRecord[] records = pixelPosList.toArray(new PixelPosRecord[pixelPosList.size()]);
        Arrays.sort(records, new YXComparator());
        return Arrays.asList(records);
    }

    public List<PixelPosRecord> computePixelPosRecords(Iterable<Record> referenceRecords) {
        return getInputRecordsSortedByPixelYX(referenceRecords);
    }

    public static Area computePixelArea(List<PixelPosRecord> pixelPosRecords, int macroPixelSize) {
        Area pixelArea = new Area();
        int i = 0;
        for (PixelPosProvider.PixelPosRecord pixelPosRecord : pixelPosRecords) {
            PixelPos pixelPos = pixelPosRecord.getPixelPos();
            if (++i <= 4) {
                LOG.info(String.format("pixel pos y=%9.4f, x=%9.4f ...", pixelPos.y, pixelPos.x));
            }

            Rectangle rectangle = new Rectangle((int) pixelPos.x - macroPixelSize / 2,
                                                (int) pixelPos.y - macroPixelSize / 2,
                                                macroPixelSize, macroPixelSize);
            pixelArea.add(new Area(rectangle));
        }
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
            double x1 = o1.pixelPos.x;
            double x2 = o2.pixelPos.x;
            if (x1 < x2) {
                return -1;
            } else if (x1 > x2) {
                return 1;
            }
            return 0;
        }
    }

    public static class PixelPosRecord {

        private final PixelPos pixelPos;
        private final Record record;
        private final long referenceTime;
        private final long eoTime;

        PixelPosRecord(PixelPos pixelPos, Record record, Date eoTime) {
            this(pixelPos, record, eoTime != null ? eoTime.getTime() : -1);
        }

        PixelPosRecord(PixelPos pixelPos, Record record, long eoTime) {
            this.record = record;
            this.pixelPos = pixelPos;
            this.referenceTime = record.getTime() != null ? record.getTime().getTime(): -1;
            this.eoTime = eoTime;
        }

        public PixelPos getPixelPos() {
            return pixelPos;
        }

        public long getReferenceTime() {
            return referenceTime;
        }

        public long getEoTime() {
            return eoTime;
        }

        public Record getRecord() {
            return record;
        }

        public long getTimeDifference() {
            long timeDiff = -1;
            if (referenceTime != -1 && eoTime != -1) {
                timeDiff = Math.abs(referenceTime - eoTime);
            }
            return timeDiff;
        }

        @Override
        public String toString() {
            return "PixelPosRecord{" +
                   "pixelPos=" + pixelPos +
                   ", record=" + record +
                   ", referenceTime=" + referenceTime +
                   ", eoTime=" + eoTime +
                   '}';
        }
    }
}
