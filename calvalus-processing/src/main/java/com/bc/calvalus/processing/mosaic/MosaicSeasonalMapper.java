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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


public class MosaicSeasonalMapper extends Mapper<NullWritable, NullWritable, TileIndexWritable, TileDataWritable> {

    Configuration jobConfig;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        jobConfig = context.getConfiguration();
        final FileSplit split = (FileSplit) context.getInputSplit();
        Path partFile = split.getPath();

        FileSystem fs = partFile.getFileSystem(jobConfig);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, partFile, jobConfig);
        try {
            TileIndexWritable key = new TileIndexWritable();
            TileDataWritable data = new TileDataWritable();
            while (reader.next(key, data)) {
                context.write(key, data);
                context.progress();
            }
        } finally {
            reader.close();
        }
    }
}
