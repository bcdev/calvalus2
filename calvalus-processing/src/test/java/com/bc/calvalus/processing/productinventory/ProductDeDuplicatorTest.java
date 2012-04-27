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
import java.io.InputStreamReader;
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
        List<ProductInventoryEntry> l1b = load("inv_fr_l1b").getAll();
        List<ProductInventoryEntry> amorgos = load("inv_fr_amo").getAll();

        System.out.println("amorgos.size() = " + amorgos.size());
        System.out.println("l1b.size() = " + l1b.size());
        List<ProductInventoryEntry> missing = ProductDeDuplicator.missing(l1b, amorgos);
        System.out.println("missing.size() = " + missing.size());
//        printInventory(missing);
    }

    private static ProductInventory load(String name) throws IOException {
        return ProductInventory.createInventory(new InputStreamReader(ProductDeDuplicatorTest.class.getResourceAsStream(name)));
    }

    private static void printInventory(List<ProductInventoryEntry> entries) {
        for (ProductInventoryEntry entry : entries) {
            System.out.println(entry.getProductName() + "\t" + entry.toCSVString());
        }
    }

}
