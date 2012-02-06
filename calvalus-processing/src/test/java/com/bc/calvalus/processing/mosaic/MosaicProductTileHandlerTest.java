/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.mosaic;

import org.junit.Test;

import static org.junit.Assert.*;

public class MosaicProductTileHandlerTest {

    @Test
    public void testGetTileProductName() throws Exception {
        assertEquals("foo-v00h00", MosaicProductTileHandler.getTileProductName("foo", 0, 0));
        assertEquals("foo-v45h13", MosaicProductTileHandler.getTileProductName("foo", 13, 45));
    }
}
