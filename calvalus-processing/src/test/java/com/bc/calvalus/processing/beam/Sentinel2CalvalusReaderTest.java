/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.beam;

import org.esa.snap.core.datamodel.Product;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class Sentinel2CalvalusReaderTest {

    @Test
    public void time() throws Exception {

        Product product = new Product("n", "d", 1, 1);
        assertNull(product.getStartTime());
        assertNull(product.getEndTime());

        String filename = "S2A_OPER_MTD_SAFL1C_PDMC_20160215T121255_R093_V20160214T093550_20160214T193550_T33PVP.xml";
        Sentinel2CalvalusReaderPlugin.Sentinel2CalvalusReader.setTimeFromFilename(product, filename);

        assertNotNull(product.getStartTime());
        assertNotNull(product.getEndTime());
        assertEquals("14-FEB-2016 09:35:50.000000", product.getStartTime().format());
        assertEquals("14-FEB-2016 19:35:50.000000", product.getEndTime().format());

    }
}