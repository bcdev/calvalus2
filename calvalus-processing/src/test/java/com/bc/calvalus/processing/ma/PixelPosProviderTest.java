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

package com.bc.calvalus.processing.ma;

import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static com.bc.calvalus.commons.DateUtils.ISO_FORMAT;
import static org.junit.Assert.assertEquals;

public class PixelPosProviderTest {

    @Test
    public void testYXComparator() throws Exception {
        Record dummyRecord = RecordUtils.create(42.0);
        PixelPosProvider.YXComparator comparator = new PixelPosProvider.YXComparator();
        List<PixelPosProvider.PixelPosRecord> list = new ArrayList<>();
        list.add(new PixelPosProvider.PixelPosRecord(new PixelPos(10, 0), dummyRecord, -1L));
        list.add(new PixelPosProvider.PixelPosRecord(new PixelPos(12, 0), dummyRecord, -1L));
        list.add(new PixelPosProvider.PixelPosRecord(new PixelPos(1, 6), dummyRecord, -1L));
        list.add(new PixelPosProvider.PixelPosRecord(new PixelPos(16, 3), dummyRecord, -1L));
        list.add(new PixelPosProvider.PixelPosRecord(new PixelPos(1, 3), dummyRecord, -1L));
        list.add(new PixelPosProvider.PixelPosRecord(new PixelPos(4, 0), dummyRecord, -1L));
        list.add(new PixelPosProvider.PixelPosRecord(new PixelPos(130, 23.498f), dummyRecord, -1L));
        list.add(new PixelPosProvider.PixelPosRecord(new PixelPos(125, 23.501f), dummyRecord, -1L));

        Collections.sort(list, comparator);
        assertEquals(new PixelPos(4, 0), list.get(0).getPixelPos());
        assertEquals(new PixelPos(10, 0), list.get(1).getPixelPos());
        assertEquals(new PixelPos(12, 0), list.get(2).getPixelPos());
        assertEquals(new PixelPos(1, 3), list.get(3).getPixelPos());
        assertEquals(new PixelPos(16, 3), list.get(4).getPixelPos());
        assertEquals(new PixelPos(1, 6), list.get(5).getPixelPos());
        assertEquals(new PixelPos(125, 23.501f), list.get(6).getPixelPos());
        assertEquals(new PixelPos(130, 23.498f), list.get(7).getPixelPos());
    }

    @Test
    public void testCalendarDayMatchupPeriod() throws Exception {

        Product product = new Product("name", "type", 10, 10);
        product.setStartTime(ProductData.UTC.create(ISO_FORMAT.parse("2017-04-08T12:00:00"), 0));
        product.setEndTime(ProductData.UTC.create(ISO_FORMAT.parse("2017-04-08T12:01:00"), 0));
        product.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 360, 180, -180.0, 90.0, 1, 1, 0.0, 0.0));
        
        // 12 hours time difference
        PixelPosProvider pixelPosProvider = new PixelPosProvider(product, null, "12.0", true);
        Record record = RecordUtils.create(new GeoPos(54, 75), ISO_FORMAT.parse("2017-04-08T22:00:00"));
        assertDateEquals("2017-04-08T10:00:00", pixelPosProvider.getMinReferenceTime(record));
        assertDateEquals("2017-04-09T10:00:00", pixelPosProvider.getMaxReferenceTime(record));

        // 30 min time difference
        pixelPosProvider = new PixelPosProvider(product, null, "0.5", true);
        record = RecordUtils.create(new GeoPos(54, 75), ISO_FORMAT.parse("2017-04-08T22:00:00"));
        assertDateEquals("2017-04-08T21:30:00", pixelPosProvider.getMinReferenceTime(record));
        assertDateEquals("2017-04-08T22:30:00", pixelPosProvider.getMaxReferenceTime(record));
        
        // 0d: day of observation (at location)
        pixelPosProvider = new PixelPosProvider(product, null, "0d", true);
        record = RecordUtils.create(new GeoPos(54, 0), ISO_FORMAT.parse("2017-04-08T02:02:02"));
        assertDateEquals("2017-04-08T00:00:00", pixelPosProvider.getMinReferenceTime(record));
        assertDateEquals("2017-04-09T00:00:00", pixelPosProvider.getMaxReferenceTime(record));

        record = RecordUtils.create(new GeoPos(54, 0), ISO_FORMAT.parse("2017-04-08T22:00:00"));
        assertDateEquals("2017-04-08T00:00:00", pixelPosProvider.getMinReferenceTime(record));
        assertDateEquals("2017-04-09T00:00:00", pixelPosProvider.getMaxReferenceTime(record));
        
        record = RecordUtils.create(new GeoPos(54, 75), ISO_FORMAT.parse("2017-04-08T22:00:00"));
        assertDateEquals("2017-04-08T19:00:00", pixelPosProvider.getMinReferenceTime(record));
        assertDateEquals("2017-04-09T19:00:00", pixelPosProvider.getMaxReferenceTime(record));

        record = RecordUtils.create(new GeoPos(54, 75), ISO_FORMAT.parse("2017-04-09T12:00:00"));
        assertDateEquals("2017-04-08T19:00:00", pixelPosProvider.getMinReferenceTime(record));
        assertDateEquals("2017-04-09T19:00:00", pixelPosProvider.getMaxReferenceTime(record));

        record = RecordUtils.create(new GeoPos(54, 75), ISO_FORMAT.parse("2017-04-08T18:00:00"));
        assertDateEquals("2017-04-07T19:00:00", pixelPosProvider.getMinReferenceTime(record));
        assertDateEquals("2017-04-08T19:00:00", pixelPosProvider.getMaxReferenceTime(record));

        // 1d: day+-1 of observation (at location) 
        pixelPosProvider = new PixelPosProvider(product, null, "1d", true);
        record = RecordUtils.create(new GeoPos(54, 75), ISO_FORMAT.parse("2017-04-08T22:00:00"));
        assertDateEquals("2017-04-07T19:00:00", pixelPosProvider.getMinReferenceTime(record));
        assertDateEquals("2017-04-10T19:00:00", pixelPosProvider.getMaxReferenceTime(record));
    }
    
    private static void assertDateEquals(String expected, long time) {
        assertEquals(expected, ISO_FORMAT.format(new Date(time)));
    }
    
}