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

import com.bc.calvalus.processing.l3.HadoopBinManager;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.AggregatorConfig;

/**
 * Partitions cube contributions by variable, spatial tile, time chunk.
 * Reducers will receive data per target chunk in ascending sequence.
 *
 * @author MB
 */
public class CubePartitioner extends Partitioner<CubeIndexWritable, CubeChunkWritable> implements Configurable {

    private Configuration conf;
    int numVariables;
    int numChunksY;
    int numChunksX;
    int numChunksT;
    int chunkSizeT;

    @Override
    public int getPartition(CubeIndexWritable binIndex, CubeChunkWritable chunk, int numPartitions) {
        int variableIndex = binIndex.getVariableIndex();
        int tileY = binIndex.getTileY();
        int tileX = binIndex.getTileX();
        int timeIndex = binIndex.getTimeIndex();
        if (numPartitions <= numVariables) {
            return variableIndex % numPartitions;
        }
        if (numPartitions <= numVariables * numChunksY) {
            return (variableIndex * numChunksY + tileY) % numPartitions;
        }
        if (numPartitions <= numVariables * numChunksY * numChunksX) {
            return ((variableIndex * numChunksY + tileY) * numChunksX + tileX) % numPartitions;
        }
        return (((variableIndex * numChunksY + tileY) * numChunksX + tileX) * numChunksT + (timeIndex / chunkSizeT)) % numPartitions;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        final BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
        final AggregatorConfig[] aggregatorConfigs = binningConfig.getAggregatorConfigs();
        if (aggregatorConfigs.length < 1 || ! "TOptCube".equals(aggregatorConfigs[0].getName())) {
            throw new IllegalArgumentException("configuration incomplete, aggregator TOptCube expected");
        }
        final AggregatorCube.Config aggregatorConfig = (AggregatorCube.Config) aggregatorConfigs[0];
        final String[] variableNames = aggregatorConfig.varNames.split(",");
        chunkSizeT = aggregatorConfig.chunkSizeT;
        final int chunkSizeY = aggregatorConfig.chunkSizeY;
        final int chunkSizeX = aggregatorConfig.chunkSizeX;
        final int yAxisLength = aggregatorConfig.yAxisLength;
        final int xAxisLength = aggregatorConfig.xAxisLength;
        final int timeAxisLength = aggregatorConfig.timeAxisLength;
        numVariables = variableNames.length;
        numChunksY = (yAxisLength + chunkSizeY - 1) / chunkSizeY;
        numChunksX = (xAxisLength + chunkSizeX - 1) / chunkSizeX;
        numChunksT = (timeAxisLength + chunkSizeT - 1) / chunkSizeT;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
