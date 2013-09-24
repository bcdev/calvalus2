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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.beam.binning.TemporalBin;
import org.esa.beam.binning.TemporalBinSource;
import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 * For formatting the results of a BEAM Level 3 Hadoop Job.
 *
 * @author Norman Fomferra
 */
public class L3TemporalBinSource implements TemporalBinSource {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String PART_FILE_PREFIX = "part-";

    private final Configuration configuration;
    private final Path partsDir;
    private final long startTime;
    private final Mapper.Context context;
    private List<PartFile> partFiles;
    private FileSystem hdfs;

    public L3TemporalBinSource(Path partsDir, Mapper.Context context) {
        this.context = context;
        this.configuration = context.getConfiguration();
        this.partsDir = partsDir;
        this.startTime = System.nanoTime();
    }

    @Override
    public int open() throws IOException {
        context.progress();
        hdfs = partsDir.getFileSystem(configuration);
        FileStatus[] parts = hdfs.listStatus(partsDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(PART_FILE_PREFIX);
            }
        });
        partFiles = readFirstIndices(parts);

        LOG.info(MessageFormat.format("start reprojection, collecting {0} parts", parts.length));

        Collections.sort(partFiles);

        return partFiles.size();
    }

    private List<PartFile> readFirstIndices(FileStatus[] parts) throws IOException {
        List<PartFile> partFiles = new ArrayList<PartFile>(parts.length);
        for (FileStatus part : parts) {
            SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, part.getPath(), configuration);
            try {
                LongWritable key = new LongWritable(-42);
                boolean more = reader.next(key);
                if (more && key.get() != -42) {
                    partFiles.add(new PartFile(part.getPath(), key.get()));
                }
            } finally {
                reader.close();
            }
        }
        return partFiles;
    }

    @Override
    public Iterator<? extends TemporalBin> getPart(int index) throws IOException {
        context.setStatus(String.format("part %d/%d", (index + 1), (partFiles.size() + 1)));
        context.progress();
        Path partFile = partFiles.get(index).getPath();
        LOG.info(MessageFormat.format("reading and reprojecting part {0}", partFile));
        SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, partFile, configuration);
        return new SequenceFileBinIterator(reader);
    }

    @Override
    public void partProcessed(int index, Iterator<? extends TemporalBin> part) throws IOException {
        context.progress();
        ((SequenceFileBinIterator) part).getReader().close();
    }

    @Override
    public void close() {
        context.progress();
        long stopTime = System.nanoTime();
        LOG.info(MessageFormat.format("stop reprojection after {0} sec", (stopTime - startTime) / 1E9));
    }

    private static class PartFile implements Comparable<PartFile> {

        private final Path path;
        private final long firstIndex;

        public PartFile(Path path, long firstIndex) {
            this.path = path;
            this.firstIndex = firstIndex;
        }

        public Path getPath() {
            return path;
        }

        @Override
        public int compareTo(PartFile other) {
            long thisVal = this.firstIndex;
            long anotherVal = other.firstIndex;
            return (thisVal < anotherVal ? -1 : (thisVal == anotherVal ? 0 : 1));
        }
    }
}
