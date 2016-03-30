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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static jdk.nashorn.internal.objects.NativeString.substring;

/**
 * Merges granules into a product. Only the original product can be re-constructed.
 */
public class GranuleMerger {

    static final String PARENT_PRODUCT_TXT = "parent_product.txt";

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("usage: GranuleMerger <inputFile> <outputDir>");
            System.exit(1);
        }

        File inputFile = new File(args[0]);
        String parenProductsContent = readParenProducts(inputFile);
        if (parenProductsContent == null) {
            throw new IllegalArgumentException("File does not contain 'parent_product.txt'. It is needed for creating the original file.");
        }
        String[] parentProductsLines = parenProductsContent.split("\n");
        String parentProductName = parentProductsLines[0];
        List<File> granuleProducts = getGranuleFiles(inputFile, parentProductsLines);

        File outputDir = new File(args[1]);
        outputDir.mkdirs();
        File outputFile = new File(outputDir, parentProductName + ".zip");
        new GranuleMerger(parentProductName, granuleProducts, outputFile).run();
    }

    static List<File> getGranuleFiles(File inputFile, String[] parentProductsLines) {
        List<File> granuleProducts = new ArrayList<>(parentProductsLines.length);
        for (int i = 1; i < parentProductsLines.length; i++) {
            String granuleProductName = parentProductsLines[i];
            File granuleFile = new File(inputFile.getParent(), granuleProductName);
            if (granuleFile.exists()) {
                granuleProducts.add(granuleFile);
            } else {
                System.out.printf("Granule product '%s' is missing. Skipping it.%n", granuleProductName);
            }
        }
        return granuleProducts;
    }

    static String readParenProducts(File inputFile) throws IOException {
        try (ZipFile zipFile = new ZipFile(inputFile)) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry zipEntry = entries.nextElement();
                String zipEntryName = zipEntry.getName();
                if (zipEntryName.endsWith(PARENT_PRODUCT_TXT)) {
                    try (InputStream input = zipFile.getInputStream(zipEntry)) {
                        return readFullyAsString(input);
                    }
                }
            }
        }
        return null;
    }

    static String readFullyAsString(InputStream inputStream) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        return result.toString("UTF-8");
    }

    private static final int BUFFER_SIZE = 8192;

    private final String parentProductName;
    private final List<File> granuleProducts;
    private final File outputFile;

    GranuleMerger(String parentProductName, List<File> granuleProducts, File outputFile) {
        this.parentProductName = parentProductName;
        this.granuleProducts = granuleProducts;
        this.outputFile = outputFile;
    }

    // DONE skip duplicat granule dirs
    // DONE skip parent-products.txt
    // DONE rename topdir
    // TODO rename product xml
    private void run() throws IOException {
        ZipEntry entry;
        try (ZipOutputStream zipOut = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)))) {
            // open first granule
            File firstFile = granuleProducts.get(0);
            System.out.println("reading: " + firstFile);
            try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(new FileInputStream(firstFile)))) {
                // copy everything, until after the granule data
                boolean granulesSeen = false;
                while ((entry = zipIn.getNextEntry()) != null) {
                    String entryName = entry.getName();
                    if (!granulesSeen || entryName.contains("GRANULE")) {
                        if (entryName.contains("GRANULE")) {
                            granulesSeen = true;
                        }
                        copyEntry(entryName, zipIn, zipOut);
                    }
                }
            }
            if (granuleProducts.size() > 1) {
                // copy granule data from all other granule files
                for (int i = 1; i < granuleProducts.size(); i++) {
                    File granuleFile = granuleProducts.get(i);
                    System.out.println("reading: " + granuleFile);
                    try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(new FileInputStream(granuleFile)))) {
                        while ((entry = zipIn.getNextEntry()) != null) {
                            String entryName = entry.getName();
                            if (entryName.contains("GRANULE") && !entryName.endsWith("GRANULE/")) {
                                copyEntry(entryName, zipIn, zipOut);
                            }
                        }
                    }
                }
            }
            // copy remaining files from first file
            System.out.println("reading: " + firstFile);
            try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(new FileInputStream(firstFile)))) {
                boolean granulesSeen = false;
                while ((entry = zipIn.getNextEntry()) != null) {
                    String entryName = entry.getName();
                    if (!granulesSeen || entryName.contains("GRANULE")) {
                        if (entryName.contains("GRANULE")) {
                            granulesSeen = true;
                        }
                    } else {
                        copyEntry(entryName, zipIn, zipOut);
                    }
                }
            }
        }
    }

    private void copyEntry(String entryName, ZipInputStream zipIn, ZipOutputStream zipOut) throws IOException {
        if (entryName.endsWith(PARENT_PRODUCT_TXT)) {
            return;
        }
        int safeIndex = entryName.indexOf(".SAFE");
        String newEntryName = parentProductName + entryName.substring(safeIndex);
        if (newEntryName.contains("SAFL1C") && newEntryName.endsWith(".xml")) {
            String[] xmlParts = newEntryName.split("/");
            String p1 = xmlParts[1].substring(0, 25);
            String p2 = xmlParts[1].substring(40);
            p2 = p2.replaceFirst("_T[0-9]{2}[A-Z]{3}", "");
            String origProcessingTime = parentProductName.substring(25, 40);

            newEntryName = xmlParts[0] +"/" + p1 + origProcessingTime + p2;
        }
        ZipEntry zipEntry = new ZipEntry(newEntryName);
        zipOut.putNextEntry(zipEntry);
        if (!zipEntry.isDirectory()) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int n;
            while ((n = zipIn.read(buffer)) > 0) {
                zipOut.write(buffer, 0, n);
            }
            zipOut.closeEntry();
        }
    }
}