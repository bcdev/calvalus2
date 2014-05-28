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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Calendar;

/**
 * Prints the content of the inventories
 */
public class InventoryLineReport {

    public static void main(String[] args) throws IOException {
        String year = args[0];
        String res = args[1];

        process(year, res);
    }

    private static void process(String year, String res) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(year + "-" + res + ".csv");
        PrintStream printStream = new PrintStream(fileOutputStream);
        process(year, res, printStream);
        printStream.close();
        fileOutputStream.close();
    }
    private static void process(String year, String res, PrintStream out) throws IOException {
        ProductInventory amorgosInventory = createInventory(getURL(year, res, "amorgos"));
        int[][] amorgosLines = countLines(amorgosInventory);
        ProductInventory l1bInventory = createInventory(getURL(year, res, "l1b"));
        int[][] l1bLines = countLines(l1bInventory);

        out.println("DayOfYear\tL1B available\tL1B toProcess\tAmorgos available\tAmorgos toProcess");
        for (int doy = 0; doy < l1bLines[0].length; doy++) {
            out.print(doy + "\t" + l1bLines[0][doy] + "\t" + l1bLines[1][doy] + "\t" + amorgosLines[0][doy] + "\t" + amorgosLines[1][doy]);
            out.println();
        }
    }

    private static int[][] countLines(ProductInventory l1bInventory) {
        int[][] lines = new int[2][367];
        for (ProductInventoryEntry productInventoryEntry : l1bInventory.getAll()) {
            int dayOfYear = productInventoryEntry.getStartTime().getAsCalendar().get(Calendar.DAY_OF_YEAR);
            lines[0][dayOfYear] += productInventoryEntry.getLength();
            lines[1][dayOfYear] += productInventoryEntry.getProcessLength();
        }
        return lines;
    }

    // /calvalus/inventory/MER_FRS_1P/v2013/2002/part-r-00000
    // /calvalus/inventory/MER_RR__1P/r03/2002/part-r-00000
    private static String getURL(String year, String res, String level) {
        String typeAndVersion;
        if ("rr".equals(res)) {
            typeAndVersion = "MER_RR__1P/r03";
        } else {
            typeAndVersion = "MER_FRS_1P/v2013";
        }
        return String.format("hdfs://master00:9000/calvalus/inventory/%s/%s/part-r-00000", typeAndVersion, year);
    }

    private static ProductInventory createInventory(String inventoryPath) throws IOException {
        Path path = new Path(inventoryPath);
        FileSystem fileSystem = path.getFileSystem(new Configuration());
        if (!fileSystem.exists(path)) {
            return null;
        }
        InputStreamReader reader = new InputStreamReader(fileSystem.open(path));
        try {
            return ProductInventory.createInventory(reader);
        } finally {
            reader.close();
        }
    }
}
