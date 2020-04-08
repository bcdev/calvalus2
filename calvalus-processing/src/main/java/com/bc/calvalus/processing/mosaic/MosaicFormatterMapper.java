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

import com.bc.calvalus.processing.beam.GpfUtils;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;


public class MosaicFormatterMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    private static final NumberFormat PART_FILE_NUMBER_FORMAT = NumberFormat.getInstance();

    static {
        PART_FILE_NUMBER_FORMAT.setMinimumIntegerDigits(5);
        PART_FILE_NUMBER_FORMAT.setGroupingUsed(false);
    }

    private static final String PART_FILE_PREFIX = "part-r-";
    Configuration jobConfig;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        jobConfig = context.getConfiguration();
        GpfUtils.init(jobConfig);

        final FileSplit split = (FileSplit) context.getInputSplit();
        Path partFile = split.getPath();

        // TODO
//        Geometry regionGeometry = JobUtils.createGeometry(jobConfig.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
//        Rectangle geometryRegion = mosaicGrid.computeRegion(regionGeometry);
//        Rectangle globalRegion = mosaicGrid.alignToTileGrid(geometryRegion);

        FileSystem fs = partFile.getFileSystem(jobConfig);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, partFile, jobConfig);

        MosaicTileHandler mosaicProductTileHandler = MosaicProductTileHandler.createHandler(context);

        TileIndexWritable key = new TileIndexWritable();
        TileDataWritable data = new TileDataWritable();
        while (reader.next(key, data)) {
            context.progress();
            mosaicProductTileHandler.handleTile(key, data);
        }
        mosaicProductTileHandler.close();
        reader.close();

        // TODO
//        context.getCounter("Product-Tiles tried", Integer.toString(tile.getMacroTileY())).increment(1);
//        context.getCounter("Product-Tiles tried", "Total").increment(1);

//        context.getCounter("Product-Tiles written", Integer.toString(tile.getMacroTileY())).increment(1);
//        context.getCounter("Product-Tiles written", "Total").increment(1);
    }


    // TODO remove ??? --- test only
    static Geometry getPartGeometry(int partitionNumber, int numPartitions) {
        double x1 = -180.0;
        double x2 = 180.0;
        double degreePerPartition = 180.0 / numPartitions;
        double y1 = 90.0 - partitionNumber * degreePerPartition;
        double y2 = 90.0 - (partitionNumber + 1) * degreePerPartition;
        return new GeometryFactory().toGeometry(new Envelope(x1, x2, y1, y2));
    }

    // TODO remove ??? --- test only
    static int getPartitionNumber(String partFilename) {
        if (!partFilename.startsWith(PART_FILE_PREFIX)) {
            throw new IllegalArgumentException("part file name is not-conforming.");
        }
        String numberPart = partFilename.substring(PART_FILE_PREFIX.length());
        try {
            Number partNumber = PART_FILE_NUMBER_FORMAT.parse(numberPart);
            return partNumber.intValue();
        } catch (ParseException e) {
            throw new IllegalArgumentException("part file number can not be parsed.");
        }
    }
}
