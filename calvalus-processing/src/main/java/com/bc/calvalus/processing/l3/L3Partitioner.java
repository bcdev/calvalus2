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

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.utils.GeometryUtils;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.esa.snap.binning.PlanetaryGrid;
import org.esa.snap.binning.operator.BinningConfig;

/**
 * Partitions the bins by their bin index.
 * Reduces will receive spatial bins of contiguous latitude ranges.
 *
 * @author Marco Zuehlke
 * @author Norman Fomferra
 */
public class L3Partitioner extends Partitioner<LongWritable, L3SpatialBin> implements Configurable {

    private Configuration conf;
    private PlanetaryGrid planetaryGrid;
    private int minRowIndex;
    private int numRowsCovered;

    @Override
    public int getPartition(LongWritable binIndex, L3SpatialBin spatialBin, int numPartitions) {
        long idx = binIndex.get();
        int partition;
        // for metadata contributions
        if (idx < 0) {
            partition = 0;
        } else {
            int row = planetaryGrid.getRowIndex(idx);
            partition = ((row - minRowIndex) * numPartitions) / numRowsCovered;
            if (partition < 0) {
                partition = 0;
            } else if (partition >= numPartitions) {
                partition = numPartitions - 1;
            }
        }
        return partition;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
        this.planetaryGrid = binningConfig.createPlanetaryGrid();
        String regionGeometry = conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY);
        Geometry roiGeometry = GeometryUtils.createGeometry(regionGeometry);
        if (roiGeometry != null && !roiGeometry.isEmpty()) {
            Envelope envelope = roiGeometry.getEnvelopeInternal();
            double minY = envelope.getMinY();
            double maxY = envelope.getMaxY();
            double minX = envelope.getMinX();
            double maxX = envelope.getMaxX();
            int maxRowIndex = planetaryGrid.getRowIndex(planetaryGrid.getBinIndex(minY, minX));
            minRowIndex = planetaryGrid.getRowIndex(planetaryGrid.getBinIndex(maxY, maxX));
            numRowsCovered = maxRowIndex - minRowIndex + 1;
        } else {
            numRowsCovered = planetaryGrid.getNumRows();
            minRowIndex = 0;
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    PlanetaryGrid getPlanetaryGrid() {
        return planetaryGrid;
    }
}
