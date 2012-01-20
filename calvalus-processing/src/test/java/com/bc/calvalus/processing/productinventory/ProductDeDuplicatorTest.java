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

package com.bc.calvalus.processing.productinventory;

import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;


public class ProductDeDuplicatorTest {

    @Test
    public void testDeduplicate() throws Exception {
        ProductInventoryEntry entry1 = ProductInventoryEntry.create("2001-10-02-13-10-11.123400",
                                                                    "2001-10-02-13-12-14.456789",
                                                                    "100", "0", "100", "Good");
        ProductInventoryEntry entry2 = ProductInventoryEntry.create("2001-10-02-13-12-15.456789",
                                                                    "2001-10-02-13-16-14.456789",
                                                                    "200", "0", "200", "Good");
        ProductInventoryEntry entry3 = ProductInventoryEntry.create("2001-10-02-13-12-15.456789",
                                                                    "2001-10-02-13-16-14.456789",
                                                                    "200", "0", "200", "Good");
        List<ProductInventoryEntry> inventory = new ArrayList<ProductInventoryEntry>();
        inventory.add(entry1);
        inventory.add(entry2);

        ProductDeDuplicator.deduplicate(inventory);
        assertEquals(2, inventory.size());
//        assertSame(deduplicate.get(0), inventory.get(0));
//        assertSame(deduplicate.get(1), inventory.get(1));
    }

    public static void main(String[] args) throws IOException {
        InputStream is = ProductDeDuplicatorTest.class.getResourceAsStream("inventory_frs_year.csv");
        Reader reader = new InputStreamReader(is);
        ProductInventory productInventory = ProductInventory.createInventory(reader);
        List<ProductInventoryEntry> inventory = productInventory.getAll();

        ProductDeDuplicator.printHistogram(inventory);
        ProductDeDuplicator.sort(inventory);
        ProductDeDuplicator.deduplicate(inventory);
        ProductDeDuplicator.printHistogram(inventory);
//        printInventory(inventory);
    }

    private static void printInventory(List<ProductInventoryEntry> entries) {
        for (ProductInventoryEntry entry : entries) {
            System.out.println(entry.toCSVString());
        }
    }

}
