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

import com.bc.calvalus.processing.JobConfigNames;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.snap.util.io.CsvReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An inventory for products. Contains only star/stop time and first and last line to be processed.
 *
 * @author MarcoZ
 */
public class ProductInventory {

    private final Map<String, ProductInventoryEntry> map;

    public static ProductInventory createInventory(Configuration conf) throws IOException {
        String inventoryPath = conf.get(JobConfigNames.CALVALUS_INPUT_INVENTORY);
        if (inventoryPath == null) {
            return null;
        }
        Path path = new Path(inventoryPath);
        FileSystem fileSystem = path.getFileSystem(conf);
        if (!fileSystem.exists(path)) {
            return null;
        }
        InputStreamReader reader = new InputStreamReader(fileSystem.open(path));
        try {
            return createInventory(reader);
        } finally {
            reader.close();
        }
    }

    public static ProductInventory createInventory(Reader reader) throws IOException {
        CsvReader csvReader = new CsvReader(reader, new char[]{'\t'});
        String[] strings = csvReader.readRecord();
        ProductInventory productInventory = new ProductInventory();
        while (strings != null) {
            productInventory.addEntry(strings);
            strings = csvReader.readRecord();
        }
        return productInventory;
    }

    public ProductInventory() {
        this.map = new HashMap<String, ProductInventoryEntry>();
    }

    void addEntry(String[] strings) {
        if (strings.length == ProductInventoryEntry.NUM_ENTRIES) {
            String productName = strings[0];
            try {
                ProductInventoryEntry entry = ProductInventoryEntry.create(strings[1], strings[2], strings[3], strings[4], strings[5], strings[6]);
                entry.setProductName(productName);
                map.put(productName, entry);
            } catch (ParseException ignore) {
            }
        }
    }

    public ProductInventoryEntry getEntry(String productName) {
        return map.get(productName);
    }

    public List<ProductInventoryEntry> getAll() {
        return new ArrayList<ProductInventoryEntry>(map.values());
    }

    public int size() {
        return map.size();
    }
}
