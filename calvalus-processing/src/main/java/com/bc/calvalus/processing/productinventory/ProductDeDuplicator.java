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

import org.esa.beam.framework.datamodel.ProductData;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Shortens products so that overlapping duplication are eliminated.
 *
 * @author MarcoZ
 * @author Martin
 */
public class ProductDeDuplicator {

    private static class ProductInventoryEntryComparator implements Comparator<ProductInventoryEntry> {
        public int compare(ProductInventoryEntry e1, ProductInventoryEntry e2) {
            return e1.getStartTime().getAsDate().compareTo(e2.getStartTime().getAsDate());
        }
    }

    private ProductDeDuplicator() {
    }

    public static void sort(List<ProductInventoryEntry> inventory) {
        Collections.sort(inventory, new ProductInventoryEntryComparator());
    }

    public static void deduplicate(List<ProductInventoryEntry> inventory) {
        long cursorMicros = 0;
        for (int i = 0; i < inventory.size(); i++) {
            ProductInventoryEntry entry = inventory.get(i);

            if (entry.getLength() <= 1 || !entry.getMessage().equalsIgnoreCase("Good")) {
                entry.setProcessLength(0);
            } else {
                long startMicros = getMJDMicros(entry.getStartTime());
                long endMicros = getMJDMicros(entry.getStopTime());
                if (cursorMicros == 0 || startMicros > cursorMicros) {
                    cursorMicros = endMicros;
                } else if (endMicros <= cursorMicros) {
                    entry.setProcessLength(0);
                } else if (startMicros <= cursorMicros) {
                    long perLineMicros = (endMicros - startMicros) / (entry.getLength() - 1);
                    double microDelta = cursorMicros - startMicros;
                    int overlap = (int) Math.round((microDelta / perLineMicros) + 1);
                    int firstPart = overlap / 2;
                    int secondPart = overlap - firstPart;

                    ProductInventoryEntry lastEntry = inventory.get(i - 1);
                    lastEntry.setProcessLength(lastEntry.getProcessLength() - firstPart);

                    entry.setProcessStartLine(secondPart);
                    entry.setProcessLength(entry.getLength() - secondPart);

                    cursorMicros = endMicros;
                }
            }
        }
    }

    private static final long SECONDS_PER_DAY = 86400;
    private static final long MICROS_PER_SECOND = 1000000;

    private static long getMJDMicros(ProductData.UTC utc) {
        long dayAsSeconds = utc.getDaysFraction() * SECONDS_PER_DAY;
        long seconds = dayAsSeconds + utc.getSecondsFraction();
        return seconds * MICROS_PER_SECOND + utc.getMicroSecondsFraction();
    }

    public static void printHistogram(List<ProductInventoryEntry> inventory) {
        int[] histo = new int[30];
        for (ProductInventoryEntry entry : inventory) {
            int processLength = entry.getProcessLength();
            int i = processLength / 500;
            histo[i]++;
        }
        System.out.println(Arrays.toString(histo));
    }

}
