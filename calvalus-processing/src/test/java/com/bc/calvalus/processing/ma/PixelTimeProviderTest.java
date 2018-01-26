/*
 * Copyright (C) 2017 Brockmann Consult GmbH (info@brockmann-consult.de) 
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

import com.bc.calvalus.commons.DateUtils;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.ProductData;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.*;

public class PixelTimeProviderTest {
    private ProductData.UTC utc1;
    private ProductData.UTC utc2;

    @Before
    public void setUp() throws Exception {
        utc1 = ProductData.UTC.parse("02-Oct-2001 10:00:00");
        utc2 = ProductData.UTC.parse("02-Oct-2001 12:00:00");
    }

    @Test
    public void testStartDiffersFromEnd() throws Exception {
        PixelTimeProvider pixelTimeProvider = PixelTimeProvider.create(utc1, utc2, 120);
        Date time;
        time = pixelTimeProvider.getTime(new PixelPos(0.5, 0.5));
        assertEquals("2001-10-02T10:00:00", DateUtils.ISO_FORMAT.format(time));
        time = pixelTimeProvider.getTime(new PixelPos(0.5, 60));
        assertEquals("2001-10-02T11:00:30", DateUtils.ISO_FORMAT.format(time));
        time = pixelTimeProvider.getTime(new PixelPos(0.5, 119.5));
        assertEquals("2001-10-02T12:00:00", DateUtils.ISO_FORMAT.format(time));
    }
    
    @Test
    public void testStartEqualsEnd() throws Exception {
        PixelTimeProvider pixelTimeProvider = PixelTimeProvider.create(utc1, utc1, 120);
        Date time;
        time = pixelTimeProvider.getTime(new PixelPos(0.5, 0.5));
        assertEquals("2001-10-02T10:00:00", DateUtils.ISO_FORMAT.format(time));
        time = pixelTimeProvider.getTime(new PixelPos(0.5, 60));
        assertEquals("2001-10-02T10:00:00", DateUtils.ISO_FORMAT.format(time));
        time = pixelTimeProvider.getTime(new PixelPos(0.5, 119.5));
        assertEquals("2001-10-02T10:00:00", DateUtils.ISO_FORMAT.format(time));
    }
    
}