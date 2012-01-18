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

package com.bc.calvalus.processing.productinventory;

import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class ProductInventoryEntryTest {

    @Test
    public void testCreate() throws Exception {
        ProductInventoryEntry entry = ProductInventoryEntry.create("2001-10-02-13-10-11.123400",
                                                                   "2001-12-02-13-12-14.456789",
                                                                   "3", "44", "bar");
        assertNotNull(entry);
        assertEquals("bar", entry.getMessage());

        assertEquals(3, entry.getStartLine());
        assertEquals(44, entry.getStopLine());
        assertNotNull(entry.getStartTime());
        assertTrue(ProductData.UTC.parse("02-Oct-2001 13:10:11.1234").equalElems(entry.getStartTime()));
        assertNotNull(entry.getStopTime());
        assertTrue(ProductData.UTC.parse("02-Dec-2001 13:12:14.456789").equalElems(entry.getStopTime()));
    }

    @Test
    public void testCreateEmpty() throws Exception {
        ProductInventoryEntry entry = ProductInventoryEntry.createEmpty("foo");
        assertNotNull(entry);
        assertEquals("foo", entry.getMessage());
        assertEquals(-1, entry.getStartLine());
        assertEquals(-1, entry.getStopLine());
    }

    @Test
    public void testCreateForProduct() throws Exception {
        Product product = new Product("name", "desc", 23, 45);

        ProductInventoryEntry entry = ProductInventoryEntry.createForProduct(product, "foo");
        assertNotNull(entry);
        assertEquals("foo", entry.getMessage());
        assertEquals(0, entry.getStartLine());
        assertEquals(44, entry.getStopLine());
        assertNull(entry.getStartTime());
        assertNull(entry.getStopTime());

        ProductData.UTC startTime = ProductData.UTC.parse("02-Jul-2001 13:10:11.123456");
        product.setStartTime(startTime);
        ProductData.UTC endTime = ProductData.UTC.parse("02-Jul-2001 13:12:14.456789");
        product.setEndTime(endTime);
        entry = ProductInventoryEntry.createForProduct(product, "foo");
        assertNotNull(entry);
        assertEquals("foo", entry.getMessage());
        assertEquals(0, entry.getStartLine());
        assertEquals(44, entry.getStopLine());
        assertNotNull(entry.getStartTime());
        assertTrue(startTime.equalElems(entry.getStartTime()));
        assertNotNull(entry.getStopTime());
        assertTrue(endTime.equalElems(entry.getStopTime()));
    }

    @Test
    public void testToCsvString() throws Exception {
        Product product = new Product("name", "desc", 23, 45);
        ProductInventoryEntry entry = ProductInventoryEntry.createForProduct(product, "foo");

        assertEquals("null\tnull\t0\t44\tfoo", entry.toCSVString());

        ProductData.UTC startTime = ProductData.UTC.parse("02-Jul-2001 13:10:11.1234");
        product.setStartTime(startTime);
        ProductData.UTC endTime = ProductData.UTC.parse("02-Jul-2001 13:12:14.5678");
        product.setEndTime(endTime);
        entry = ProductInventoryEntry.createForProduct(product, "bar");

        assertEquals("2001-07-02-13-10-11.123400\t2001-07-02-13-12-14.567800\t0\t44\tbar", entry.toCSVString());
    }
}
