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

import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.junit.Test;

import static org.junit.Assert.*;

public class ProductInventoryEntryTest {

    @Test
    public void testCreate() throws Exception {
        ProductInventoryEntry entry = ProductInventoryEntry.create("2001-10-02-13-10-11.123400",
                                                                   "2001-12-02-13-12-14.456789",
                                                                   "65", "3", "44", "bar");
        assertNotNull(entry);
        assertEquals("bar", entry.getMessage());

        assertEquals(65, entry.getLength());
        assertEquals(3, entry.getProcessStartLine());
        assertEquals(44, entry.getProcessLength());
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
        assertEquals(0, entry.getLength());
        assertEquals(0, entry.getProcessStartLine());
        assertEquals(0, entry.getProcessLength());
    }

    @Test
    public void testCreateForProduct() throws Exception {
        Product product = new Product("name", "desc", 23, 45);

        ProductInventoryEntry entry = ProductInventoryEntry.createForGoodProduct(product, "foo");
        assertNotNull(entry);
        assertEquals("foo", entry.getMessage());
        assertEquals(45, entry.getLength());
        assertEquals(0, entry.getProcessStartLine());
        assertEquals(45, entry.getProcessLength());
        assertNotNull(entry.getStartTime());
        ProductData.UTC utcZero = new ProductData.UTC();
        assertTrue(utcZero.equalElems(entry.getStartTime()));
        assertNotNull(entry.getStopTime());
        assertTrue(utcZero.equalElems(entry.getStopTime()));

        ProductData.UTC startTime = ProductData.UTC.parse("02-Jul-2001 13:10:11.123456");
        product.setStartTime(startTime);
        ProductData.UTC endTime = ProductData.UTC.parse("02-Jul-2001 13:12:14.456789");
        product.setEndTime(endTime);
        entry = ProductInventoryEntry.createForGoodProduct(product, "foo");
        assertNotNull(entry);
        assertEquals("foo", entry.getMessage());
        assertEquals(45, entry.getLength());
        assertEquals(0, entry.getProcessStartLine());
        assertEquals(45, entry.getProcessLength());
        assertNotNull(entry.getStartTime());
        assertTrue(startTime.equalElems(entry.getStartTime()));
        assertNotNull(entry.getStopTime());
        assertTrue(endTime.equalElems(entry.getStopTime()));
    }

    @Test
    public void testToCsvString() throws Exception {
        Product product = new Product("name", "desc", 23, 45);
        ProductInventoryEntry entry = ProductInventoryEntry.createForGoodProduct(product, "foo");

        assertEquals("2000-01-01-00-00-00.000000\t2000-01-01-00-00-00.000000\t45\t0\t45\tfoo", entry.toCSVString());

        ProductData.UTC startTime = ProductData.UTC.parse("02-Jul-1990 13:10:11.1234");
        product.setStartTime(startTime);
        ProductData.UTC endTime = ProductData.UTC.parse("02-Jul-2001 13:12:14.5678");
        product.setEndTime(endTime);
        entry = ProductInventoryEntry.createForGoodProduct(product, "bar");

        assertEquals("1990-07-02-13-10-11.123400\t2001-07-02-13-12-14.567800\t45\t0\t45\tbar", entry.toCSVString());
    }
}
