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

package com.bc.calvalus.processing.ta;

import com.bc.calvalus.binning.Aggregator;
import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.binning.BinningContext;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3TemporalBin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * For formatting the results of a BEAM Level 3 Hadoop Job.
 *
 *  @author Norman
 */
public class TAFormatter {
    private static final String PART_FILE_PREFIX = "part-r-";
    private static final Logger LOG = CalvalusLogger.getLogger();

    private BinningContext binningContext;
    private File outputFile;

    private final Logger logger;
    private final Configuration hadoopConf;
    private Path partsDir;

    public TAFormatter(Logger logger, Configuration hadoopConf) {
        this.logger = logger;
        this.hadoopConf = hadoopConf;
    }

    public int format(File outputFile, L3Config l3Config, String hadoopJobOutputDir) throws Exception {

        this.outputFile = outputFile;

        partsDir = new Path(hadoopJobOutputDir);
        binningContext = l3Config.getBinningContext();
        final BinManager binManager = binningContext.getBinManager();
        final int aggregatorCount = binManager.getAggregatorCount();
        if (aggregatorCount == 0) {
            throw new IllegalArgumentException("Illegal binning context: aggregatorCount == 0");
        }

        logger.info("aggregators.length = " + aggregatorCount);
        for (int i = 0; i < aggregatorCount; i++) {
            Aggregator aggregator = binManager.getAggregator(i);
            logger.info("aggregators." + i + " = " + aggregator);
        }

        outputFile.getParentFile().mkdirs();
        // todo - nf/nf 18.04.2011: read sequence file ...
        return 0;
    }

    void hahaha() throws IOException {
        final FileSystem hdfs = partsDir.getFileSystem(hadoopConf);
        final FileStatus[] parts = hdfs.listStatus(partsDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(PART_FILE_PREFIX);
            }
        });

        LOG.info(MessageFormat.format("start reprojection, collecting {0} parts", parts.length));

        Arrays.sort(parts);

        for (FileStatus part : parts) {
            Path partFile = part.getPath();
            SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, partFile, hadoopConf);
            LOG.info(MessageFormat.format("reading and reprojecting part {0}", partFile));
            try {
                while (true) {
                    LongWritable binIndex = new LongWritable();
                    L3TemporalBin temporalBin = new L3TemporalBin();
                    if (!reader.next(binIndex, temporalBin)) {
                        break;
                    }
                }
            } finally {
                reader.close();
            }
        }
    }

}
