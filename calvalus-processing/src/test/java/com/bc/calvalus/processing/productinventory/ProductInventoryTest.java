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

import org.junit.Test;

import java.io.StringReader;

import static org.junit.Assert.*;

public class ProductInventoryTest {

    @Test
    public void testCreateInventory() throws Exception {
        String content = "the/path\t2001-07-02-13-10-11.123400\t2001-07-02-13-12-14.567800\t0\t44\tbar";

        StringReader reader = new StringReader(content);
        ProductInventory inventory = ProductInventory.createInventory(reader);

        assertNotNull(inventory);
        assertEquals(1, inventory.size());

        ProductInventoryEntry entry = inventory.getEntry("the/wrong/path");
        assertNull(entry);

        entry = inventory.getEntry("the/path");
        assertNotNull(entry);
        assertEquals("bar", entry.getMessage());
        assertEquals(0, entry.getStartLine());
        assertEquals(44, entry.getStopLine());
    }
}
