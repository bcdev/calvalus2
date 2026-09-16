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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.jna.ptr.NativeLongByReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.blosc.BufferSizes;
import org.blosc.IBloscDll;
import org.blosc.JBlosc;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.opengis.geometry.DirectPosition;

import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * Utility functions to write content to zarr files in JSON, uncompressed data, and compressed data
 *
 * @author MB
 */
public class ZarrWriter {

    private final ObjectMapper jsonFactory;
    private final ByteOrder byteOrder;
    private final String compressorName;
    private final Map<String,String> compressorParameters;
    private final Path rootPath;
    private final FileSystem fileSystem;

    public ZarrWriter(ObjectMapper jsonFactory, ByteOrder byteOrder, String compressorName, Map<String,String> compressorParameters, Configuration conf, String destRoot) throws IOException {
        this.jsonFactory = jsonFactory;
        this.byteOrder = byteOrder;
        this.compressorName = compressorName;
        this.compressorParameters = compressorParameters;
        this.rootPath = new Path(destRoot);
        this.fileSystem = rootPath.getFileSystem(conf);
    }

    public void writeJsonFile(String destDir, String variableName, String filename, ObjectNode json) throws IOException {
        final Path variablePath = new Path(rootPath, variableName);
        fileSystem.mkdirs(variablePath);
        final Path filePath = new Path(variablePath, filename);
        try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fileSystem.create(filePath)))) {
            jsonFactory.writeValue(out, json);
        }
    }
    public void writeJsonFile(String destDir, String filename, ObjectNode json) throws IOException {
        fileSystem.mkdirs(rootPath);
        final Path filePath = new Path(rootPath, filename);
        try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fileSystem.create(filePath)))) {
            jsonFactory.writeValue(out, json);
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

        final Path variablePath = new Path(rootPath, variableName);
        fileSystem.mkdirs(variablePath);
        final Path filePath = new Path(variablePath, "0");
        try (final OutputStream out = fileSystem.create(filePath)) {
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

    public void writeLatLonValuesToZarr(String variableName, Product product, String destDir) throws IOException {
        final GeoCoding geoCoding = product.getSceneGeoCoding();
        final int size = "lat".equals(variableName) ? product.getSceneRasterHeight() : product.getSceneRasterWidth();
        final double[] values = new double[size];
        GeoPos geoPos = new GeoPos();
        PixelPos pipo = new PixelPos();
        pipo.setLocation(0.5, 0.5);
        if ("lat".equals(variableName)) {
            for (int y = 0; y < size; y++) {
                pipo.y = y + 0.5;
                geoCoding.getGeoPos(pipo, geoPos);
                values[y] = geoPos.lat;
            }
        } else {
            for (int x = 0; x < size; x++) {
                pipo.x = x + 0.5;
                geoCoding.getGeoPos(pipo, geoPos);
                values[x] = geoPos.lat;
            }
        }
        final ImageOutputStream byteStream = new MemoryCacheImageOutputStream(new ByteArrayOutputStream());
        byteStream.setByteOrder(byteOrder);
        byteStream.writeDoubles(values, 0, values.length);
        byteStream.seek(0);
        final Path variablePath = new Path(rootPath, variableName);
        fileSystem.mkdirs(variablePath);
        final Path filePath = new Path(variablePath, "0");
        try (final OutputStream out = fileSystem.create(filePath)) {
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
            String variable,
            int currentTileY, int currentTileX, int currentTileT, byte[] currentData,
            String destDir
    ) throws IOException {
        final Path variablePath = new Path(rootPath, variable);
        fileSystem.mkdirs(variablePath);
        final Path filePath = new Path(variablePath, currentTileT + "." + currentTileY + "." + currentTileX);

        if ("zlib".equals(compressorName)) {
            final int level = compressorParameters.containsKey("level")
                    ? Integer.parseInt(compressorParameters.get("level"))
                    : Deflater.DEFAULT_COMPRESSION;
            Deflater deflater = new Deflater(level);
            try (final DeflaterOutputStream out = new DeflaterOutputStream(
                    fileSystem.create(filePath),
                    deflater
            )) {
                out.write(currentData);
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
            final int inputSize = currentData.length;
            final int outputSize = inputSize + JBlosc.OVERHEAD;
            final ByteBuffer inputBuffer = ByteBuffer.wrap(currentData);
            final ByteBuffer outBuffer = ByteBuffer.allocate(outputSize);
            final int i = JBlosc.compressCtx(clevel, shuffle, 1, inputBuffer, inputSize, outBuffer, outputSize, cname, blocksize, 1);
            final BufferSizes bs = cbufferSizes(outBuffer);
            try (OutputStream out = fileSystem.create(filePath)) {
                out.write(outBuffer.array(), 0, (int) bs.getCbytes());
            }
        } else {
            try (final OutputStream out = fileSystem.create(filePath)) {
                out.write(currentData);
            }
        }
    }

    public void writeTimeToZarr(
            String variable,
            int currentTileT, byte[] currentData,
            String destDir
    ) throws IOException {
        Path variablePath = new Path(rootPath, variable);
        fileSystem.mkdirs(variablePath);
        Path filePath = new Path(variablePath, String.valueOf(currentTileT));
        try (final OutputStream out = fileSystem.create(filePath)) {
            out.write(currentData);
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
