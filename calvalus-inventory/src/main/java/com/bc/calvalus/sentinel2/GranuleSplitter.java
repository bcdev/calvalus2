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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;


/**
 * Splits a S2 product into individual granules
 * <p>
 * Parent-Product:
 * S2A_OPER_PRD_MSIL1C_PDMC_20160215T121255_R093_V20160214T093550_20160214T093550.SAFE/
 * - S2A_OPER_MTD_SAFL1C_PDMC_20160215T121255_R093_V20160214T093550_20160214T093550.xml
 * - GRANULE/
 * - - S2A_OPER_MSI_L1C_TL_SGS__20160214T150139_A003379_T33PVP_N02.01/
 * - - - S2A_OPER_MTD_L1C_TL_SGS__20160214T150139_A003379_T33PVP.xml
 * <p>
 * Single-Granule-Product:
 * S2A_OPER_PRD_MSIL1C_PDMC_20160214T150139_R093_V20160214T093550_20160214T093550_T33PVP.SAFE/
 * - S2A_OPER_MTD_SAFL1C_PDMC_20160214T150139_R093_V20160214T093550_20160214T093550_T33PVP.xml
 * - GRANULE/
 * - - S2A_OPER_MSI_L1C_TL_SGS__20160214T150139_A003379_T33PVP_N02.01/
 * - - - S2A_OPER_MTD_L1C_TL_SGS__20160214T150139_A003379_T33PVP.xml
 * <p>
 * Changes for single granule products are:
 * - the processing time of the product is replaced by the processing time of the granule
 * - the tile identifier is added to the new product name
 * - a history.txt file is added
 */
public class GranuleSplitter {

    private static final boolean ADD_ZIP_SUFFIX = true;

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("usage: GranuleSplitter <inputFile|-> <outputDir>");
            System.exit(1);
        }

        InputStream inputStream;
        if (args[0].equals("-")) {
            inputStream = System.in;
        } else {
            File inputFile = new File(args[0]);
            if (inputFile.exists()) {
                if (inputFile.canRead()) {
                    inputStream = new FileInputStream(inputFile);
                } else {
                    throw new IllegalArgumentException("Can not read input file: " + inputFile.getAbsolutePath());
                }
            } else {
                throw new IllegalArgumentException("Input file does not exist: " + inputFile.getAbsolutePath());
            }
        }


        File outputDir = new File(args[1]);
        outputDir.mkdirs();
        new GranuleSplitter(inputStream, outputDir).run();
    }

    private static final int BUFFER_SIZE = 8192;
    private static final Pattern TILE_PATTERN = Pattern.compile(".*(_T[0-9]{2}[A-Z]{3}).*");

    private final InputStream inputStream;
    private final File outputDir;

    private GranuleSplitter(InputStream inputStream, File outputDir) {
        this.inputStream = inputStream;
        this.outputDir = outputDir;
    }

    private void run() throws IOException {
        Map<String, GranuleWriter> granuleWriters = new HashMap<>();
        String productName = null;
        try (ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(inputStream))) {
            List<ZipEntryBuffer> zipEntryBuffers = new ArrayList<>();
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                String entryName = entry.getName();
                if (detectGranule(entryName) == null) {
                    ZipEntryBuffer zipEntryBuffer = new ZipEntryBuffer(entryName, (int) entry.getSize(), zipIn);
                    zipEntryBuffers.add(zipEntryBuffer);

                    for (GranuleWriter granuleWriter : granuleWriters.values()) {
                        writeBufferToWriter(zipEntryBuffer, granuleWriter);
                    }
                } else {
                    if (productName == null) {
                        productName = entryName.split("/")[0];
                        int i = productName.indexOf(".SAFE");
                        if (i > -1) {
                            productName = productName.substring(0, i);
                        }
                    }
                    // entry belongs to a specific granule
                    GranuleSpec granuleSpec = GranuleSpec.parse(entryName);
                    GranuleWriter granuleWriter = granuleWriters.get(granuleSpec.getGranuleName());
                    if (granuleWriter == null) {

                        granuleWriter = new GranuleWriter(granuleSpec);
                        granuleWriters.put(granuleSpec.getGranuleName(), granuleWriter);

                        // write buffered entries
                        for (ZipEntryBuffer zipEntryBuffer : zipEntryBuffers) {
                            writeBufferToWriter(zipEntryBuffer, granuleWriter);
                        }
                    }
                    copyToAWriter(entryName, zipIn, granuleWriter);
                }
            }
        }
        List<String> newProductNames = new ArrayList<>(granuleWriters.size());
        for (GranuleWriter granuleWriter : granuleWriters.values()) {
            newProductNames.add(granuleWriter.getOutputFile().getName());
        }
        for (GranuleWriter granuleWriter : granuleWriters.values()) {
            granuleWriter.writeParentProductInfo(productName, newProductNames);
            granuleWriter.close();
        }
    }

    static String detectGranule(String entryName) {
        if (entryName.contains("GRANULE")) {
            Matcher matcher = TILE_PATTERN.matcher(entryName);
            if (matcher.matches()) {
                return matcher.group(1);
            }
        }
        return null;
    }

    private static void copyToAWriter(String name, ZipInputStream zipIn, GranuleWriter writer) throws IOException {
        copyToAllWriters(name, zipIn, Collections.singletonList(writer));
    }

    private static void copyToAllWriters(String name, ZipInputStream zipIn, Collection<GranuleWriter> writers) throws IOException {
        for (GranuleWriter writer : writers) {
            writer.putNextEntry(name);
        }
        if (!name.endsWith("/")) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int n;
            while ((n = zipIn.read(buffer)) > 0) {
                for (GranuleWriter writer : writers) {
                    writer.write(buffer, n);
                }
            }
            for (GranuleWriter writer : writers) {
                writer.closeEntry();
            }
        }
    }

    private static void writeBufferToWriter(ZipEntryBuffer zipEntryBuffer, GranuleWriter writer) throws IOException {
        writer.putNextEntry(zipEntryBuffer.getName());
        if (!zipEntryBuffer.getName().endsWith("/")) {
            byte[] buffer = zipEntryBuffer.getBuffer();
            writer.write(buffer, buffer.length);
            writer.closeEntry();
        }
    }

    private class GranuleWriter implements AutoCloseable {

        private final ZipOutputStream zipOutputStream;
        private final GranuleSpec granuleSpec;
        private final File outputFile;

        GranuleWriter(GranuleSpec granuleSpec) throws IOException {
            this.granuleSpec = granuleSpec;
            File outputFileTemp = new File(outputDir, getGranuleFilename(0));
            int counter = 1;
            while (outputFileTemp.exists()) {
                outputFileTemp = new File(outputDir, getGranuleFilename(counter));
                counter++;
            }
            System.out.println("writing to: " + outputFileTemp);
            this.outputFile = outputFileTemp;
            this.zipOutputStream = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));
        }

        private String getGranuleFilename(int counter) {
            String filename = granuleSpec.getTopDirName();
            if (counter > 0) {
                filename += "_" + counter;
            }
            if (ADD_ZIP_SUFFIX) {
                filename += ".zip";
            }
            return filename;
        }

        File getOutputFile() {
            return outputFile;
        }

        void putNextEntry(String entryName) throws IOException {
            String[] entyNameParts = entryName.split("/");
            entyNameParts[0] = granuleSpec.getTopDirName() + ".SAFE";
            if (entyNameParts.length > 1 && entyNameParts[1].contains("SAFL1C") && entyNameParts[1].endsWith(".xml")) {
                entyNameParts[1] = granuleSpec.convertXmlName(entyNameParts[1]);
            }
            String newEntryName = String.join("/", entyNameParts);
            if (entryName.endsWith("/")) {
                newEntryName += "/";
            }
            zipOutputStream.putNextEntry(new ZipEntry(newEntryName));
        }

        void write(byte[] buffer, int n) throws IOException {
            zipOutputStream.write(buffer, 0, n);
        }

        void closeEntry() throws IOException {
            zipOutputStream.closeEntry();
        }

        public void writeParentProductInfo(String productName, List<String> newProductNames) throws IOException {
            putNextEntry(productName + ".SAFE/" + GranuleMerger.PARENT_PRODUCT_TXT);
            StringBuilder sb = new StringBuilder();
            sb.append(productName);
            sb.append("\n");
            for (String newProductName : newProductNames) {
                sb.append(newProductName);
                sb.append("\n");
            }
            byte[] buffer = sb.toString().getBytes(Charset.forName("UTF-8"));

            write(buffer, buffer.length);
        }

        @Override
        public void close() throws IOException {
            zipOutputStream.close();
        }
    }

    private class ZipEntryBuffer {
        private String name;
        private byte[] buffer;

        ZipEntryBuffer(String name, int size, ZipInputStream zipIn) throws IOException {
            this.name = name;
            this.buffer = new byte[size];
            int offset = 0;
            int n;
            int len = buffer.length;
            while ((n = zipIn.read(buffer, offset, len)) > 0) {
                offset += n;
                len -= n;
            }
        }

        public String getName() {
            return name;
        }

        public byte[] getBuffer() {
            return buffer;
        }
    }

    static class GranuleSpec {
        private final String topDirName;
        private final String granuleName;
        private final String tileId;
        private final String granuleProcessingTime;

        GranuleSpec(String topDirName, String granuleName, String tileId, String granuleProcessingTime) {
            this.topDirName = topDirName;
            this.granuleName = granuleName;
            this.tileId = tileId;
            this.granuleProcessingTime = granuleProcessingTime;
        }

        String getTopDirName() {
            return topDirName;
        }

        String getGranuleName() {
            return granuleName;
        }

        String getTileId() {
            return tileId;
        }

        String getGranuleProcessingTime() {
            return granuleProcessingTime;
        }

        String convertXmlName(String xmlName) {
            return convertName(xmlName, granuleProcessingTime, tileId, ".xml") + ".xml";
        }

        private static String convertName(String name, String granuleProcessingTime, String tileId, String extension) {
            String p1 = name.substring(0, 25);
            String p2 = name.substring(40);
            int i = p2.indexOf(extension);
            if (i > -1) {
                p2 = p2.substring(0, i);
            }
            return p1 + granuleProcessingTime + p2 + tileId;
        }

        static GranuleSpec parse(String entryName) {
            String[] entrySplit = entryName.split("/");
            if (entrySplit.length >= 3) {
                String productName = entrySplit[0];
                String granuleName = entrySplit[2];

                String granuleProcessingTime = granuleName.substring(25, 40);
                String tileId = detectGranule(entryName);

                String topDirName = convertName(productName, granuleProcessingTime, tileId, ".SAFE");
                return new GranuleSpec(topDirName, granuleName, tileId, granuleProcessingTime);
            }
            throw new IllegalArgumentException("unable to parse granule spec");
        }
    }
}
