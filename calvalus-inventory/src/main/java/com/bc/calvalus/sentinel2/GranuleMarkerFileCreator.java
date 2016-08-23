/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.sentinel2;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;


/**
 * Write content of the marker files for given full products.
 * These contain the names of all granules.
 */
public class GranuleMarkerFileCreator {

    private static final boolean ADD_ZIP_SUFFIX = true;

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("usage: GranuleMarkerFileCreator <inputFile>");
            System.exit(1);
        }
        File productFile = new File(args[0]);
        if (!productFile.exists()) {
            throw new IllegalArgumentException("Input file does not exist: " + productFile.getAbsolutePath());
        }

        writeMarkerFile(productFile);
    }

    private static void writeMarkerFile(File productFile) throws IOException {
        List<String> newProductNames = new ArrayList<>();
        try (ZipFile zipFile = new ZipFile(productFile)) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                String zipEntryName = entries.nextElement().getName();
                if (isGranuleXml(zipEntryName)) {
                    GranuleSplitter.GranuleSpec granuleSpec = GranuleSplitter.GranuleSpec.parse(zipEntryName);
                    String granuleFilename = getGranuleFilename(granuleSpec);
                    newProductNames.add(granuleFilename);
                }
            }
        }
        writeMarkerFileContent(newProductNames);
    }

    private static void writeMarkerFileContent(List<String> newProductNames) throws IOException {
        try (Writer fileWriter = new PrintWriter(System.out)) {
            for (String newProductName : newProductNames) {
                fileWriter.write(newProductName + "\n");
            }
        }
    }


    private static String getGranuleFilename(GranuleSplitter.GranuleSpec granuleSpec) {
        String filename = granuleSpec.getTopDirName();
        if (ADD_ZIP_SUFFIX) {
            filename += ".zip";
        }
        return filename;
    }

    private static boolean isGranuleXml(String entryName) {
        return entryName.contains("GRANULE") && entryName.matches(".*(_T[0-9]{2}[A-Z]{3}).xml$");
    }
}
