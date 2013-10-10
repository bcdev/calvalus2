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

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        List<FileStatus> fileStatusOfParts = listStatus(job);
        CalvalusLogger.getLogger().info("Total part files to process : " + fileStatusOfParts.size());
        Map<Path, List<String>> directoryToHostMap = new HashMap<Path, List<String>>();
        for (FileStatus fileStatusOfPart : fileStatusOfParts) {

            Path partPath = fileStatusOfPart.getPath();
            FileSystem fs = partPath.getFileSystem(job.getConfiguration());
            long length = fileStatusOfPart.getLen();
            BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatusOfPart, 0, length);
            String[] hosts = blkLocations[0].getHosts();

            Path directoryPath = partPath.getParent();
            List<String> allHosts = directoryToHostMap.get(directoryPath);
            if (allHosts == null) {
                allHosts = new ArrayList<String>();
                directoryToHostMap.put(directoryPath, allHosts);
            }
            allHosts.addAll(Arrays.asList(hosts));
        }
        List<InputSplit> splits = new ArrayList<InputSplit>();
        for (Map.Entry<Path, List<String>> pathListEntry : directoryToHostMap.entrySet()) {
            Path partDir = pathListEntry.getKey();
            List<String> allHosts = pathListEntry.getValue();

            // length has no meaning
            FileSplit split = new FileSplit(partDir, 0, 42, allHosts.toArray(new String[allHosts.size()]));
            splits.add(split);
        }
        CalvalusLogger.getLogger().info("Total part directories to process : " + splits.size());
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
