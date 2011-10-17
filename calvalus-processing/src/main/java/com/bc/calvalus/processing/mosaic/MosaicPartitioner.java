/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.mosaic;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import org.esa.beam.util.math.MathUtils;

/**
 * Partitions the tiles by their y index.
 * Reduces will receive tiles of contiguous latitude ranges.
 *
 * @author Marco Zuehlke
 */
public class MosaicPartitioner extends Partitioner<TileIndexWritable, TileDataWritable> implements Configurable {

    private Configuration conf;
    private int minRowIndex;
    private int numRowsCovered;

    @Override
    public int getPartition(TileIndexWritable tileIndex, TileDataWritable tileData, int numPartitions) {
        int row = tileIndex.getTileY();
        int partition = ((row - minRowIndex) * numPartitions) / numRowsCovered;
        if (partition < 0) {
            partition = 0;
        } else if (partition >= numPartitions) {
            partition = numPartitions - 1;
        }
        return partition;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        String regionGeometry = conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY);
        Geometry roiGeometry = JobUtils.createGeometry(regionGeometry);
        if (roiGeometry != null && !roiGeometry.isEmpty()) {
            Envelope envelope = roiGeometry.getEnvelopeInternal();
            int maxRowIndex = MathUtils.ceilInt(90.0 - envelope.getMinY());
            minRowIndex = MathUtils.floorInt(90.0 - envelope.getMaxY());
            numRowsCovered = maxRowIndex - minRowIndex;
        } else {
            numRowsCovered = 180;
            minRowIndex = 0;
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

}
