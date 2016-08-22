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
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;


/**
 * Write marker files for given full products.
 * These contain the names of all granules.
 */
public class GranuleMarkerFileCreator {

    private static final boolean ADD_ZIP_SUFFIX = true;
    private static File outputDir = new File(".");

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("usage: GranuleMarkerFileCreator <inputFile|inputDir>");
            System.exit(1);
        }
        File input = new File(args[0]);
        if (!input.exists()) {
            throw new IllegalArgumentException("Input file does not exist: " + input.getAbsolutePath());
        }
        File[] productFiles;
        if (input.isDirectory()) {
            productFiles = input.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.startsWith("S2") &&
                            new File(dir, name).isFile() &&
                            (name.matches(".*_[0-9]{8}T[0-9]{6}.zip$") || name.matches(".*_[0-9]{8}T[0-9]{6}$"));
                }
            });
        } else {
            productFiles = new File[]{input};
        }

        for (File productFile : productFiles) {
            try {
                writeMarkerFile(productFile);
            } catch (IOException e) {
                System.out.println("productFile = " + productFile);
                e.printStackTrace();
            }
        }
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
        writeMarkerFile(productFile.getName(), newProductNames);
    }

    private static void writeMarkerFile(String parentProductName, List<String> newProductNames) throws IOException {
        File markerFile = new File(outputDir, "_" + parentProductName);
        try (FileWriter fileWriter = new FileWriter(markerFile)) {
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

    static boolean isGranuleXml(String entryName) {
        return entryName.contains("GRANULE") && entryName.matches(".*(_T[0-9]{2}[A-Z]{3}).xml$");
    }
}
