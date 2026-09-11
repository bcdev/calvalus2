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
 * Reduces ...
 *
 * @author MB
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
            final ByteOrder byteOrder = "bigendian".equals(aggregatorConfig.encoding) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
            final String compressorName = aggregatorConfig.compression.split(",")[0];
            final Map<String,String> compressorParameters = new HashMap<>();
            for (String pair: aggregatorConfig.compression.substring(aggregatorConfig.compression.indexOf(",") + 1).split(",")) {
                String[] elements = pair.split(":");
                compressorParameters.put(elements[0], elements[1]);
            }
            final String destDir = conf.get("calvalus.output.dir");

            final ZarrWriter zarrWriter = new ZarrWriter(null, byteOrder, compressorName, compressorParameters);

            // repeatedly receives chunks with key and array

            int currentVariableIndex = -1;
            int currentTileY = -1;
            int currentTileX = -1;
            int currentTileT = -1;
            int sizeY = -1;
            int sizeX = -1;
            int sizeT = -1;
            byte[] currentData = null;

            while (context.nextKey()) {
                CubeIndexWritable key = context.getCurrentKey();
                CubeChunkWritable value = context.getCurrentValue();

                // determines whether (v, ty, tx, t) is a contribution to the next chunk, flushes the current one, initialises a new one

                if (key.getVariableIndex() != currentVariableIndex
                        || key.getTileY() != currentTileY
                        || key.getTileY() != currentTileX
                        || key.getTimeIndex() / chunkSizeT != currentTileT
                ) {
                    if (currentVariableIndex != -1) {
                        zarrWriter.writeChunkToZarr(variableNames[currentVariableIndex], currentTileY, currentTileX, currentTileT, currentData, destDir);
                    }

                    // initialising t1, y1, x1, lt, ly, lx, provisioning of data array with length lt

                    currentVariableIndex = key.getVariableIndex();
                    currentTileY = key.getTileY();
                    currentTileX = key.getTileX();
                    currentTileT = key.getTimeIndex() / chunkSizeT;
                    sizeY = Math.min(chunkSizeY, yAxisLength - currentTileY * chunkSizeY);
                    sizeX = Math.min(chunkSizeX, xAxisLength - currentTileX * chunkSizeX);
                    sizeT = Math.min(chunkSizeT, timeAxisLength - currentTileT * chunkSizeT);
                    currentData = new byte[sizeT * sizeY * sizeX * value.getTypeLength()];
                    for (int i=0; i<sizeT * sizeY * sizeX; ++i) {
                        System.arraycopy(value.getFillBytes(), 0, currentData, i*value.getTypeLength(), value.getTypeLength());
                    }
                    LOG.info("collecting contributions of chunk "
                                     + variableNames[currentVariableIndex]
                                     + "[" + currentTileT + "," + currentTileY + "," + currentTileX + "]");
                }

                // appends data to the current chunk, i.e. reads values into ly, lx array, sets into data array at t-t1 (t of index)

                final byte[] bytes = value.getBuffer();
                final int timePositionInChunk = key.getTimeIndex() - currentTileT * chunkSizeT;
                System.arraycopy(bytes, 0, currentData, timePositionInChunk * sizeY * sizeX * value.getTypeLength(), sizeY * sizeX * value.getTypeLength());  // TODO check whether to multiply with type size
            }
            if (currentVariableIndex != -1) {
                zarrWriter.writeChunkToZarr(variableNames[currentVariableIndex], currentTileY, currentTileX, currentTileT, currentData, destDir);
            }

        } finally {
            cleanup(context);
        }
    }

}
