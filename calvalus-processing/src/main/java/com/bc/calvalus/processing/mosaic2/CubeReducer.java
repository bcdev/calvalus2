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
import com.bc.calvalus.processing.l3.HadoopBinManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.binning.AggregatorConfig;
import org.esa.snap.binning.operator.BinningConfig;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;

/**
 * Purpose of this reducer is to generate time-chunked data cubes from a time series of inputs, e.g. daily coverages.
 *
 * The reducer receives tiled contributions to a time chunk for one variable and spatial tile in temporal order.
 * The mapper has processed an input product in the target projection with the target variables.
 * Parameters per product (table input format) are the time index and whether this is the selected input to write
 * the metadata of the cube.
 * Parameters common to all calls are chunk sizes and dimensions in T, Y, X, byte order, compression, global metadata.
 *
 * The following classes implement the functions to generate the cube:
 * - CubeMapper cuts one input into spatial chunks, streams them to reducers ordered by variable, spatial chunk, time.
 * - CubePartitioner sorts the keys of variable, y tile, x tile, time and partitions them by time chunk
 * - CubeReducer collects the contributions for one chunk in temporal order, writes it, initialises for the next chunk
 * - CubeIndexWritable container for the key of variable, y tile, x tile, time index
 * - CubeDataWritable container for the chunk contribution as float array on the mapper side and byte array at reducer
 * - AggregatorCube container for the common parameters, an aggregator only to pass it in a kind of level 3 request
 * - MetadataCollector functions to create JSON record for .zarray, .zattrs, .zmetadata
 * - ZarrWriter functions to write JSON record to file, to write coordinates, to compress and write data chunk
 *
 * Key is variable index, y tile index, x tile index, time index.
 * The chunk is written as byte array on the mapper side, considering byte order for the target cube already.
 * The value transmitted from mapper to reducer is a quadrupel of num bytes per value, num bytes of the transmitted
 * array, fill value encoded bytes, data encoded as bytes.
 * The quadrupel is compressed for transfer and is uncompressed on the reducer side. This is lzw level 1 for speed.
 *
 * The time of the input is streamed as a separate key-value pair.
 * The key is #variables, 0, 0, time index.
 * The value is a quadrupel of 8, 8, the bytes for double -1 as fill value, and the bytes for the double with the
 * number of days between 2000-01-01 and the acquisition time of the input product of this mapper.
 *
 * A mapper that had an input labelled to provide the metadata has written all .zxxx files, y and x.
 *
 * @author MB
 */
public class CubeReducer extends Reducer<CubeIndexWritable, CubeChunkWritable, NullWritable, NullWritable> {

    private static final Logger LOG = CalvalusLogger.getLogger();

    /**
     * This reducer receives a stream of variable tiles for time steps in temporal order.
     * It detects a change in the chunk, writes the previous chunk to a zarr chunk file, and initialises the next
     * target chunk buffer. It places the contribution in the right position in the target chunk buffer.
     * If the variable is "time" then the buffer is just a 1-D array and the contribution is the acquisition time.
     */

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        try {

            // reads configuration with variableNames, time extend, height, width, chunk sizes in T, Y, X

            final Configuration conf = context.getConfiguration();
            final BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
            final AggregatorConfig[] aggregatorConfigs = binningConfig.getAggregatorConfigs();
            if (aggregatorConfigs.length < 1 || !"TimeChunkedCube".equals(aggregatorConfigs[0].getName())) {
                throw new IllegalArgumentException("configuration incomplete, aggregator TimeChunkedCube expected");
            }
            final TimeChunkedCubeAggregator.Config aggregatorConfig = (TimeChunkedCubeAggregator.Config) aggregatorConfigs[0];
            final String[] variableNames = aggregatorConfig.varNames.split(",");
            //final int timeShape = aggregatorConfig.timeShape;
            final int yShape = aggregatorConfig.yShape;
            final int xShape = aggregatorConfig.xShape;
            final int timeChunks = aggregatorConfig.timeChunks;
            final int yChunks = aggregatorConfig.yChunks;
            final int xChunks = aggregatorConfig.xChunks;
            final ByteOrder byteOrder = "bigendian".equals(aggregatorConfig.byteOrder) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
            final String compressorName = aggregatorConfig.compression.split(",")[0];
            final Map<String,String> compressorParameters = new HashMap<>();
            for (String pair: aggregatorConfig.compression.substring(aggregatorConfig.compression.indexOf(",") + 1).split(",")) {
                String[] elements = pair.split(":");
                compressorParameters.put(elements[0], elements[1]);
            }
            final String destDir = conf.get("calvalus.output.dir");

            final ZarrWriter zarrWriter = new ZarrWriter(null, byteOrder, compressorName, compressorParameters, conf, destDir);

            // repeatedly receives chunks with key and array

            int currentVariableIndex = -1;
            int currentTileY = -1;
            int currentTileX = -1;
            int currentTileT = -1;
            int sizeY = -1;
            int sizeX = -1;
            byte[] currentData = null;

            int contributionCounter = 0;

            while (context.nextKey()) {
                CubeIndexWritable key = context.getCurrentKey();
                CubeChunkWritable value = context.getCurrentValue();
                if (contributionCounter == 0) {
                    LOG.info("Cube reducer started for initial key " + key);
                }
                ++contributionCounter;

                // determines whether (v, ty, tx, t) is a contribution to the next chunk, flushes the current one, initialises a new one

                if (key.getVariableIndex() != currentVariableIndex
                        || key.getTileY() != currentTileY
                        || key.getTileX() != currentTileX
                        || key.getTimeIndex() / timeChunks != currentTileT
                ) {
                    if (currentVariableIndex != -1) {
                        if (currentVariableIndex < variableNames.length) {
                            zarrWriter.writeChunkToZarr(variableNames[currentVariableIndex], currentTileY, currentTileX, currentTileT, currentData, destDir);
                        } else {
                            zarrWriter.writeTimeToZarr("time", currentTileT, currentData, destDir);
                        }
                    }

                    // initialising key elements and currentData, filling currentData with fill value

                    currentVariableIndex = key.getVariableIndex();
                    currentTileY = key.getTileY();
                    currentTileX = key.getTileX();
                    currentTileT = key.getTimeIndex() / timeChunks;
                    sizeY = Math.min(yChunks, yShape - currentTileY * yChunks);
                    sizeX = Math.min(xChunks, xShape - currentTileX * xChunks);
                    if (currentVariableIndex < variableNames.length) {
                        currentData = new byte[timeChunks * yChunks * xChunks * value.getTypeLength()];
                        for (int i = 0; i < timeChunks * yChunks * xChunks; ++i) {
                            System.arraycopy(value.getFillBytes(), 0, currentData, i * value.getTypeLength(), value.getTypeLength());
                        }
                        LOG.fine("collecting contributions of chunk "
                                         + variableNames[currentVariableIndex]
                                         + "[" + currentTileT + "," + currentTileY + "," + currentTileX + "]");
                    } else {  // time variable
                        currentData = new byte[timeChunks * value.getTypeLength()];
                        for (int i = 0; i < timeChunks; ++i) {
                            System.arraycopy(value.getFillBytes(), 0, currentData, i * value.getTypeLength(), value.getTypeLength());
                        }
                        LOG.fine("collecting contributions of chunk time" + "[" + currentTileT + "]");
                    }
                }

                // appends data to the current chunk, i.e. reads values into ly, lx array, sets into data array at t-t1 (t of index)

                final byte[] bytes = value.getBuffer();
                final int timePositionInChunk = key.getTimeIndex() - currentTileT * timeChunks;
                if (currentVariableIndex < variableNames.length) {
                    if (sizeX == xChunks) {
                        System.arraycopy(bytes, 0, currentData, timePositionInChunk * yChunks * xChunks * value.getTypeLength(), sizeY * sizeX * value.getTypeLength());
                    } else {  // right border with shorter lines
                        for (int y = 0; y < sizeY; ++y) {
                            System.arraycopy(bytes,
                                             y * sizeX * value.getTypeLength(),
                                             currentData,
                                             timePositionInChunk * yChunks * xChunks * value.getTypeLength() + y * xChunks * value.getTypeLength(),
                                             sizeX * value.getTypeLength()
                            );
                        }
                    }
                } else {  // time variable
                    System.arraycopy(bytes, 0, currentData, timePositionInChunk * value.getTypeLength(), value.getTypeLength());
                }
            }
            // no more inputs, flush last chunk
            if (currentVariableIndex != -1) {
                if (currentVariableIndex < variableNames.length) {
                    zarrWriter.writeChunkToZarr(variableNames[currentVariableIndex], currentTileY, currentTileX, currentTileT, currentData, destDir);
                } else {
                    zarrWriter.writeTimeToZarr("time", currentTileT, currentData, destDir);
                }
            }
            LOG.info(contributionCounter + " contribution tiles processed");

        } finally {
            cleanup(context);
        }
    }

}
