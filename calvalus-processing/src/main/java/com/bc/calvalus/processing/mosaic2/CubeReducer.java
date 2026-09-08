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
import com.bc.calvalus.processing.hadoop.MetadataSerializer;
import com.bc.calvalus.processing.l3.HadoopBinManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.binning.AggregatorConfig;
import org.esa.snap.binning.BinningContext;
import org.esa.snap.binning.TemporalBinner;
import org.esa.snap.binning.cellprocessor.CellProcessorChain;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.core.datamodel.MetadataElement;

import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Logger;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * Reduces ...
 *
 * @author Martin
 */
public class CubeReducer extends Reducer<CubeIndexWritable, CubeChunkWritable, NullWritable, NullWritable> {

    private static final Logger LOG = CalvalusLogger.getLogger();

    /**
     * TBD revise text from other reducer
     * This iterator receives a stream of micro tiles of spatial bins but has to deliver the temporal bins one by one.
     * All micro tiles for one tile key are provided in sequence.
     * The iterator reads them and performs the temporal aggregation for the complete array keeping an array of TemporalBins.
     * Additional difficulty is that both SpatialBin and TemporalBin may be null, spatial if the pixel contrib is
     * missing and temporal if the pixel is outside of the area.
     * The inner loop is the cursor running over temporalBins positions. The outer loop is over micro tiles, i.e. context keys.
     * In addition, the iterator stops if the next micro tile is part of another macro tile. Then, a call to nextTile()
     * re-initialises the iterator to continue.
     * The iterator uses lookahead.
     */

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        try {

            // reads configuration with variableNames, time extend, height, width, chunk sizes in T, Y, X

            final Configuration conf = context.getConfiguration();
            final BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
            final AggregatorConfig[] aggregatorConfigs = binningConfig.getAggregatorConfigs();
            if (aggregatorConfigs.length < 1 || !"TOptCube".equals(aggregatorConfigs[0].getName())) {
                throw new IllegalArgumentException("configuration incomplete, aggregator TOptCube expected");
            }
            final AggregatorCube.Config aggregatorConfig = (AggregatorCube.Config) aggregatorConfigs[0];
            final String[] variableNames = aggregatorConfig.varNames.split(",");
            final int timeAxisLength = aggregatorConfig.timeAxisLength;
            final int yAxisLength = aggregatorConfig.yAxisLength;
            final int xAxisLength = aggregatorConfig.xAxisLength;
            final int chunkSizeT = aggregatorConfig.chunkSizeT;
            final int chunkSizeY = aggregatorConfig.chunkSizeY;
            final int chunkSizeX = aggregatorConfig.chunkSizeX;
            final String destDir = conf.get("calvalus.output.dir");

            // repeatedly receives chunks with key and array

            int currentVariableIndex = -1;
            int currentTileY = -1;
            int currentTileX = -1;
            int currentTileT = -1;
            int sizeY = -1;
            int sizeX = -1;
            int sizeT = -1;
            Object currentData = null;

            while (context.nextKey()) {
                CubeIndexWritable key = context.getCurrentKey();
                CubeChunkWritable value = context.getCurrentValue();
                Object elems = value.getSamples();

                // determines whether (v, ty, tx, t) is a contribution to the current chunk, flushes the current one, initialises a new one

                if (key.getVariableIndex() != currentVariableIndex
                        || key.getTileY() != currentTileY
                        || key.getTileY() != currentTileX
                        || key.getTimeIndex() / chunkSizeT != currentTileT
                ) {
                    if (currentVariableIndex != -1) {

                        // flushing means writing a zarr file, name determined by variable name, tt, ty, tx

                        String destination = destDir + "/" + variableNames[currentVariableIndex] + "/" + currentTileT + "." + currentTileY + "." + currentTileX;
                        Files.createDirectories(Paths.get(destDir + "/" + variableNames[currentVariableIndex]));

                        final ImageOutputStream byteStream = new MemoryCacheImageOutputStream(new ByteArrayOutputStream());
                        byteStream.setByteOrder(ByteOrder.LITTLE_ENDIAN);  // TODO parameter
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

                        final int level = 1;
                        Deflater deflater = new Deflater(level);
                        try (final DeflaterOutputStream out = new DeflaterOutputStream(
                                new FileOutputStream(destination),
                                deflater
                        )) {
                            final byte[] buffer = new byte[4096];
                            while (true) {
                                final int count = byteStream.read(buffer);
                                if (count == 0) {
                                    break;
                                }
                                out.write(buffer, 0, count);
                            }
                        }
                        deflater.end();

                        // TODO distinguish compression methods, allow their specification in parameters

//                        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
//                        passThrough(is, baos);
//                        final byte[] inputBytes = baos.toByteArray();
//                        final int inputSize = inputBytes.length;
//                        final int outputSize = inputSize + JBlosc.OVERHEAD;
//                        final ByteBuffer inputBuffer = ByteBuffer.wrap(inputBytes);
//                        final ByteBuffer outBuffer = ByteBuffer.allocate(outputSize);
//                        final int i = JBlosc.compressCtx(clevel, shuffle, 1, inputBuffer, inputSize, outBuffer, outputSize, cname, blocksize, 1);
//                        final BufferSizes bs = cbufferSizes(outBuffer);
//                        byte[] compressedChunk = Arrays.copyOfRange(outBuffer.array(), 0, (int) bs.getCbytes());
//                        os.write(compressedChunk);

                    }

                    // initialising means determination of t1, y1, x1, lt, ly, lx, provisioning of data array with length lt

                    currentVariableIndex = key.getVariableIndex();
                    currentTileY = key.getTileY();
                    currentTileX = key.getTileX();
                    currentTileT = key.getTimeIndex() / chunkSizeT;
                    sizeY = Math.min(chunkSizeY, yAxisLength - currentTileY * chunkSizeY);
                    sizeX = Math.min(chunkSizeX, xAxisLength - currentTileX * chunkSizeX);
                    sizeT = Math.min(chunkSizeT, timeAxisLength - currentTileT * chunkSizeT);
                    if (elems instanceof float[]) {
                        currentData = new float[sizeT * sizeY * sizeX];
                        // TBD init with fill value
                    } else if (elems instanceof int[]) {
                        currentData = new int[sizeT * sizeY * sizeX];
                    } else if (elems instanceof short[]) {
                        currentData = new short[sizeT * sizeY * sizeX];
                    } else if (elems instanceof byte[]) {
                        currentData = new byte[sizeT * sizeY * sizeX];
                    } else if (elems instanceof double[]) {
                        currentData = new double[sizeT * sizeY * sizeX];
                    } else if (elems instanceof long[]) {
                        currentData = new long[sizeT * sizeY * sizeX];
                    } else {
                        throw new IllegalArgumentException("unknown array type of " + elems);
                    }
                    LOG.info("collecting contributions of chunk "
                                     + variableNames[currentVariableIndex]
                                     + " [" + currentTileT + "," + currentTileY + "," + currentTileX + "]");
                }

                // appends data to the current chunk, i.e. reads values into ly, lx array, sets into data array at t-t1 (t of index)

                final int timePositionInChunk = key.getTimeIndex() - currentTileT * chunkSizeT;
                System.arraycopy(elems, 0, currentData, timePositionInChunk * sizeY * sizeX, sizeY * sizeX);
            }
        } finally {
            cleanup(context);
        }
    }
}
