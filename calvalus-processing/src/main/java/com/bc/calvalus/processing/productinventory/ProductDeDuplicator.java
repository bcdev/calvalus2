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

import org.esa.snap.core.datamodel.ProductData;

import java.util.ArrayList;
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

    public static List<ProductInventoryEntry> missing(List<ProductInventoryEntry> src, List<ProductInventoryEntry> target) {
        List<ProductInventoryEntry> missing = new ArrayList<ProductInventoryEntry>();
        for (ProductInventoryEntry srcEntry : src) {
            boolean found = false;
            int processLength = srcEntry.getProcessLength();
            int processStartLine = srcEntry.getProcessStartLine();

            if (processLength != 0) {

                int a0 = (processStartLine / 64) * 64;
                int a1 = (processStartLine + processLength - 1 + 64 - 1) / 64 * 64;

                long startMicros = getMJDMicros(srcEntry.getStartTime());
                long endMicros = getMJDMicros(srcEntry.getStopTime());
                long perLineMicros = (endMicros - startMicros) / (srcEntry.getLength() - 1);

                long newStartTime = startMicros + a0 * perLineMicros;
                for (ProductInventoryEntry e : target) {
                    if (e.getProcessLength() != 0) {
                        long targetStartMicros = getMJDMicros(e.getStartTime());
                        if (targetStartMicros == newStartTime || targetStartMicros == startMicros) {
                            found = true;
                            break;
                        }
                    }
                }
                if (!found) {
                    long newStopTime = startMicros + a1 * perLineMicros;
                    for (ProductInventoryEntry e : target) {
                        if (e.getProcessLength() != 0) {
                            long targetStopMicros = getMJDMicros(e.getStopTime());
                            if (targetStopMicros == newStopTime) {
                                found = true;
                                break;
                            }
                        }
                    }
                }
                if (!found) {
                    missing.add(srcEntry);
                }
            }
        }
        return missing;
    }

    public static void deduplicate(List<ProductInventoryEntry> inventory) {
        long cursorMicros = 0;
        ProductInventoryEntry cursorEntry = null;

        for (ProductInventoryEntry entry : inventory) {
            long startMicros = getMJDMicros(entry.getStartTime());
            long endMicros = getMJDMicros(entry.getStopTime());
            if (entry.getLength() <= 1 ||
                    !entry.getMessage().equalsIgnoreCase("Good") ||
                    startMicros == 0 ||
                    endMicros == 0) {
                entry.setProcessLength(0);
            } else {
                if (cursorMicros == 0 || startMicros > cursorMicros) {
                    cursorMicros = endMicros;
                    cursorEntry = entry;
                } else if (endMicros <= cursorMicros) {
                    entry.setProcessLength(0);
                } else if (startMicros <= cursorMicros) {
                    long perLineMicros = (endMicros - startMicros) / (entry.getLength() - 1);
                    double microOverlap = cursorMicros - startMicros;
                    int overlapLines = (int) Math.round((microOverlap / perLineMicros) + 1);
                    int firstPartLines = overlapLines / 2;
                    int secondPartLines = overlapLines - firstPartLines;

                    cursorEntry.setProcessLength(cursorEntry.getProcessLength() - firstPartLines);

                    entry.setProcessStartLine(secondPartLines);
                    entry.setProcessLength(entry.getLength() - secondPartLines);

                    cursorMicros = endMicros;
                    cursorEntry = entry;
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

    public static DeduplicationStatistics getStatistics(List<ProductInventoryEntry> inventory) {
        int linesTotal = 0;
        int linesToProcess = 0;
        for (ProductInventoryEntry entry : inventory) {
            linesTotal += entry.getLength();
            linesToProcess += entry.getProcessLength();
        }
        return new DeduplicationStatistics(linesTotal, linesToProcess);
    }

    public static class DeduplicationStatistics {

        private final int linesTotal;
        private final int linesToProcess;

        public DeduplicationStatistics(int linesTotal, int linesToProcess) {
            this.linesTotal = linesTotal;
            this.linesToProcess = linesToProcess;
        }

        public int getLinesTotal() {
            return linesTotal;
        }

        public int getLinesToProcess() {
            return linesToProcess;
        }

        public int getLinesDuplicated() {
            return linesTotal - linesToProcess;
        }
    }
}
