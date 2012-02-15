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

import com.bc.calvalus.binning.TemporalBin;
import com.bc.calvalus.binning.TemporalBinSource;
import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Logger;

/**
 * For formatting the results of a BEAM Level 3 Hadoop Job.
 *
 * @author Norman Fomferra
 */
public class L3TemporalBinSource implements TemporalBinSource {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String PART_FILE_PREFIX = "part-r-";

    private final Configuration configuration;
    private final Path partsDir;
    private final long startTime;
    private FileStatus[] parts;
    private FileSystem hdfs;

    public L3TemporalBinSource(Configuration configuration, Path partsDir) {
        this.configuration = configuration;
        this.partsDir = partsDir;
        startTime = System.nanoTime();
    }

    @Override
    public int open() throws IOException {

        hdfs = partsDir.getFileSystem(configuration);
        parts = hdfs.listStatus(partsDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(PART_FILE_PREFIX);
            }
        });

        LOG.info(MessageFormat.format("start reprojection, collecting {0} parts", parts.length));

        Arrays.sort(parts);

        return parts.length;
    }

    @Override
    public Iterator<? extends TemporalBin> getPart(int index) throws IOException {
        final FileStatus part = parts[index];
        Path partFile = part.getPath();
        LOG.info(MessageFormat.format("reading and reprojecting part {0}", partFile));
        SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, partFile, configuration);
        return new SequenceFileBinIterator(reader);
    }

    @Override
    public void partProcessed(int index, Iterator<? extends TemporalBin> part) throws IOException {
        ((SequenceFileBinIterator) part).getReader().close();
    }

    @Override
    public void close() {
        long stopTime = System.nanoTime();
        LOG.info(MessageFormat.format("stop reprojection after {0} sec", (stopTime - startTime) / 1E9));
    }
}
