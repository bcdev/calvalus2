/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.mosaic2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.esa.snap.core.datamodel.Product;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.opengis.geometry.DirectPosition;

import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import com.sun.jna.ptr.NativeLongByReference;
import org.blosc.JBlosc;
import org.blosc.BufferSizes;
import org.blosc.IBloscDll;

/**
 * Functions to write content to zarr files, json, uncompressed data, and compressed data
 *
 * @author MB
 */
public class ZarrWriter {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private final ObjectMapper objectMapper;
    private final ByteOrder byteOrder;
    private final String compressorName;
    private final Map<String,String> compressorParameters;

    public ZarrWriter(ObjectMapper objectMapper, ByteOrder byteOrder, String compressorName, Map<String,String> compressorParameters) {
        this.objectMapper = objectMapper;
        this.byteOrder = byteOrder;
        this.compressorName = compressorName;
        this.compressorParameters = compressorParameters;
    }

    public void writeJsonFile(String destDir, String variableName, String filename, ObjectNode json) throws IOException {
        Files.createDirectories(Paths.get(destDir, variableName));
        try (BufferedWriter out = new BufferedWriter(new FileWriter(new File(new File(destDir, variableName), filename)))) {
            objectMapper.writeValue(out, json);
        }
    }
    public void writeJsonFile(String destDir, String filename, ObjectNode json) throws IOException {
        Files.createDirectories(Paths.get(destDir));
        try (BufferedWriter out = new BufferedWriter(new FileWriter(new File(destDir, filename)))) {
            objectMapper.writeValue(out, json);
        }
    }

    public void writeXYValuesToZarr(String variableName, Product product, String destDir) throws IOException {
        AffineTransform2D transform = (AffineTransform2D) product.getSceneGeoCoding().getImageToMapTransform();
        final boolean isY = "y".equals(variableName);
        final int ordinateIndex = isY ? 1 : 0;
        final DirectPosition pos = new DirectPosition2D(0.0, 0.0);
        final DirectPosition2D targetPos = new DirectPosition2D();
        final double[] values = new double[isY ? product.getSceneRasterHeight() : product.getSceneRasterWidth()];
        for (int i = 0; i < values.length; ++i) {
            pos.setOrdinate(ordinateIndex, i+0.5);
            transform.transform(pos, targetPos);
            values[i] = targetPos.getOrdinate(ordinateIndex);
        }
        final ImageOutputStream byteStream = new MemoryCacheImageOutputStream(new ByteArrayOutputStream());
        byteStream.setByteOrder(byteOrder);
        byteStream.writeDoubles(values, 0, values.length);
        byteStream.seek(0);
        Files.createDirectories(Paths.get(destDir, variableName));
        try (final FileOutputStream out = new FileOutputStream(new File(new File(destDir, variableName), "0"))) {
            final byte[] buffer = new byte[4096];
            while (true) {
                final int count = byteStream.read(buffer);
                if (count <= 0) {
                    break;
                }
                out.write(buffer, 0, count);
            }
        }
    }

    public void writeChunkToZarr(
            String variableNames,
            int currentTileY, int currentTileX, int currentTileT, Object currentData,
            String destDir
    ) throws IOException {
        String destination = destDir + "/" + variableNames + "/" + currentTileT + "." + currentTileY + "." + currentTileX;
        Files.createDirectories(Paths.get(destDir + "/" + variableNames));

        final ImageOutputStream byteStream = new MemoryCacheImageOutputStream(new ByteArrayOutputStream());
        byteStream.setByteOrder(byteOrder);
        if (currentData instanceof float[]) {
            byteStream.writeFloats((float[]) currentData, 0, ((float[]) currentData).length);
        } else if (currentData instanceof int[]) {
            byteStream.writeInts((int[]) currentData, 0, ((int[]) currentData).length);
        } else if (currentData instanceof short[]) {
            byteStream.writeShorts((short[]) currentData, 0, ((short[]) currentData).length);
        } else if (currentData instanceof byte[]) {
            byteStream.write((byte[]) currentData, 0, ((byte[]) currentData).length);
        } else if (currentData instanceof double[]) {
            byteStream.writeDoubles((double[]) currentData, 0, ((double[]) currentData).length);
        } else if (currentData instanceof long[]) {
            byteStream.writeLongs((long[]) currentData, 0, ((long[]) currentData).length);
        } else {
            throw new IllegalArgumentException("unexpected data type " + currentData);
        }
        byteStream.seek(0);

        if ("zlib".equals(compressorName)) {
            final int level = compressorParameters.containsKey("level")
                    ? Integer.parseInt(compressorParameters.get("level"))
                    : Deflater.DEFAULT_COMPRESSION;
            Deflater deflater = new Deflater(level);
            try (final DeflaterOutputStream out = new DeflaterOutputStream(
                    new FileOutputStream(destination),
                    deflater
            )) {
                final byte[] buffer = new byte[4096];
                while (true) {
                    final int count = byteStream.read(buffer);
                    if (count <= 0) {
                        break;
                    }
                    out.write(buffer, 0, count);
                }
            }
            deflater.end();
        } else if ("blosc".equals(compressorName)) {
            final int clevel = compressorParameters.containsKey("clevel")
                    ? Integer.parseInt(compressorParameters.get("clevel"))
                    : 5;
            final int shuffle = compressorParameters.containsKey("shuffle")
                    ? Integer.parseInt(compressorParameters.get("shuffle"))
                    : 1;
            final String cname = compressorParameters.containsKey("cname")
                    ? compressorParameters.get("cname")
                    : "lz4";
            final int blocksize = compressorParameters.containsKey("blocksize")
                    ? Integer.parseInt(compressorParameters.get("blocksize"))
                    : 0;
            final int inputSize = (int) byteStream.length();
            // TODO one copy too much
            byte[] inputBytes = new byte[inputSize];
            byteStream.read(inputBytes);
            final int outputSize = inputSize + JBlosc.OVERHEAD;
            final ByteBuffer inputBuffer = ByteBuffer.wrap(inputBytes);
            final ByteBuffer outBuffer = ByteBuffer.allocate(outputSize);
            final int i = JBlosc.compressCtx(clevel, shuffle, 1, inputBuffer, inputSize, outBuffer, outputSize, cname, blocksize, 1);
            final BufferSizes bs = cbufferSizes(outBuffer);
            try (FileOutputStream out = new FileOutputStream(destination)) {
                out.write(outBuffer.array(), 0, (int) bs.getCbytes());
            }
        }
    }

    private BufferSizes cbufferSizes(ByteBuffer cbuffer) {
            NativeLongByReference nbytes = new NativeLongByReference();
            NativeLongByReference cbytes = new NativeLongByReference();
            NativeLongByReference blocksize = new NativeLongByReference();
            IBloscDll.blosc_cbuffer_sizes(cbuffer, nbytes, cbytes, blocksize);
            BufferSizes bs = new BufferSizes(nbytes.getValue().longValue(),
                                             cbytes.getValue().longValue(),
                                             blocksize.getValue().longValue());
            return bs;
        }

}
