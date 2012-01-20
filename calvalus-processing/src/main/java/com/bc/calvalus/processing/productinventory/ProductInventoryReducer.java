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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

/**
 * Creates a inventory from the result of the mapper and
 * uses the de-duplication tool to remove overlaps.
 *
 * @author MarcoZ
 */
public class ProductInventoryReducer extends Reducer<Text, Text, Text, Text> {

    private final ArrayList<ProductInventoryEntry> inventory = new ArrayList<ProductInventoryEntry>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] splits = value.toString().split("\t");
            try {
                ProductInventoryEntry entry = ProductInventoryEntry.create(splits[1], splits[2], splits[3], splits[4], splits[5], splits[6]);
                entry.setProductName(splits[0]);
                inventory.add(entry);
            } catch (ParseException ignore) {
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        ProductDeDuplicator.sort(inventory);
        ProductDeDuplicator.deduplicate(inventory);
        for (ProductInventoryEntry entry : inventory) {
            context.write(new Text(entry.getProductName()), new Text(entry.toCSVString()));
        }
    }
}
