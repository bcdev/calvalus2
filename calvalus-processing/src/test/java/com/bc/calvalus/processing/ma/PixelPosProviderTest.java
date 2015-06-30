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

import org.esa.snap.framework.datamodel.PixelPos;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

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
}