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

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.hadoop.NoRecordReader;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A input format that generate a single input split for the l3 job.
 * All hosts holding data will be added to a single split.
 * these resulting single map task will use all parts files from this directory.
 */
public class L3FormatterInputFormat extends FileInputFormat {

    /**
     * Generate the list of files and make them into a FileSplit.
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<FileStatus> fileStatuses = listStatus(job);
        List<InputSplit> splits = new ArrayList<InputSplit>(1);
        Set<String> allHosts = new HashSet<String>();
        for (FileStatus file : fileStatuses) {
            Path path = file.getPath();
            FileSystem fs = path.getFileSystem(job.getConfiguration());
            long length = file.getLen();
            BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
            String[] hosts = blkLocations[0].getHosts();
            allHosts.addAll(Arrays.asList(hosts));
        }
        Path[] inputPaths = FileInputFormat.getInputPaths(job);
        // length has no meaning
        splits.add(new FileSplit(inputPaths[0], 0, 42, allHosts.toArray(new String[allHosts.size()])));
        return splits;
    }

    /**
     * Creates a {@link com.bc.calvalus.processing.hadoop.NoRecordReader} because records are not used with this input format.
     */
    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoRecordReader();
    }
}
