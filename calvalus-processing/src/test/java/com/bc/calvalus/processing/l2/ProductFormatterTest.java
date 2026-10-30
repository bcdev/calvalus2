/*
 * Copyright (C) 2026 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 */

package com.bc.calvalus.processing.l2;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProductFormatterTest {

    @Test
    public void preparesUncompressedNetcdfFileForSeaGridWriter() {
        ProductFormatter formatter = new ProductFormatter(
                "example",
                ProductFormatter.FORMAT_NETCDF4_SEAGRID,
                "none");

        assertEquals(ProductFormatter.FORMAT_NETCDF4_SEAGRID, formatter.getOutputFormat());
        assertEquals("example.nc", formatter.getProductFilename());
        assertEquals("example.nc", formatter.getOutputFilename());
        assertEquals("", formatter.getOutputCompression());
    }
}
