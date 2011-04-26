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

import com.bc.calvalus.binning.BinningContext;
import com.bc.calvalus.binning.BinningGrid;
import com.bc.calvalus.binning.TemporalBin;
import com.bc.calvalus.binning.TemporalBinProcessor;
import com.bc.calvalus.binning.TemporalBinReprojector;
import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import java.awt.Rectangle;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

/**
 * Reprojects the output of the {@code L3Tool} (reducer parts stored as sequence files).
 *
 * @author Norman Fomferra
 */
public class L3Reprojector {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String PART_FILE_PREFIX = "part-r-";

    // todo - no good: TemporalBinProcessor must know that width=binningGrid.getNumRows()*2 and height=binningGrid.getNumRows()

    public static void reproject(Configuration configuration,
                                 BinningContext binningContext,
                                 Rectangle pixelRegion,
                                 Path partsDir,
                                 TemporalBinProcessor temporalBinProcessor) throws Exception {

        long startTime = System.nanoTime();

        final FileSystem hdfs = partsDir.getFileSystem(configuration);
        final FileStatus[] parts = hdfs.listStatus(partsDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(PART_FILE_PREFIX);
            }
        });

        LOG.info(MessageFormat.format("start reprojection, collecting {0} parts", parts.length));

        Arrays.sort(parts);

        temporalBinProcessor.begin(binningContext);
        for (FileStatus part : parts) {
            Path partFile = part.getPath();
            SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, partFile, configuration);
            LOG.info(MessageFormat.format("reading and reprojecting part {0}", partFile));
            try {
                reprojectPart(binningContext, pixelRegion, reader, temporalBinProcessor);
                // todo - use instead the following method, it can be tested! (nf)
                // reprojectPart(binningContext, pixelRegion, new TemporalBinIterator(reader), temporalBinProcessor);
            } finally {
                reader.close();
            }
        }
        temporalBinProcessor.end(binningContext);

        long stopTime = System.nanoTime();
        LOG.info(MessageFormat.format("stop reprojection after {0} sec", (stopTime - startTime) / 1E9));
    }

    public static void reprojectPart(BinningContext binningContext,
                                     Rectangle pixelRegion,
                                     SequenceFile.Reader temporalBinReader,
                                     TemporalBinProcessor temporalBinProcessor) throws Exception {
        final int y1 = pixelRegion.y;
        final int y2 = pixelRegion.y + pixelRegion.height - 1;
        final int x1 = pixelRegion.x;
        final int x2 = pixelRegion.x + pixelRegion.width - 1;
        final BinningGrid binningGrid = binningContext.getBinningGrid();
        final int gridWidth = binningGrid.getNumRows() * 2;
        final int gridHeight = binningGrid.getNumRows();

        int lastRowIndex = -1;
        final List<TemporalBin> binRow = new ArrayList<TemporalBin>();
        while (true) {
            LongWritable binIndex = new LongWritable();
            TemporalBin temporalBin = new TemporalBin();
            if (!temporalBinReader.next(binIndex, temporalBin)) {
                if (lastRowIndex >= y1 && lastRowIndex <= y2) {
                    TemporalBinReprojector.reprojectRow(binningContext,
                                                        pixelRegion, lastRowIndex, binRow,
                                                        temporalBinProcessor,
                                                        gridWidth, gridHeight);
                }
                if (lastRowIndex >= y1 && y2 <= y2 && y2 != (lastRowIndex + 1)) {
                    for (int y = lastRowIndex + 1; y < y2; y++) {
                        for (int x = x1; x <= x2; x++) {
                            temporalBinProcessor.processMissingBin(x - x1, y - y1);
                        }
                    }
                }
                binRow.clear();
                break;
            }
            int rowIndex = binningGrid.getRowIndex(binIndex.get());
            if (rowIndex != lastRowIndex) {
                if (lastRowIndex >= y1 && lastRowIndex <= y2) {
                    TemporalBinReprojector.reprojectRow(binningContext,
                                                        pixelRegion, lastRowIndex, binRow,
                                                        temporalBinProcessor,
                                                        gridWidth, gridHeight);
                }
                if (lastRowIndex >= y1 && rowIndex <= y2 && rowIndex != (lastRowIndex + 1)) {
                    for (int y = lastRowIndex + 1; y < rowIndex; y++) {
                        for (int x = x1; x <= x2; x++) {
                            temporalBinProcessor.processMissingBin(x - x1, y - y1);
                        }
                    }
                }
                binRow.clear();
                lastRowIndex = rowIndex;
            }
            temporalBin.setIndex(binIndex.get());
            binRow.add(temporalBin);
        }
    }

    static final class SequenceFileBinIterator implements Iterator<TemporalBin> {
        private final SequenceFile.Reader reader;
        private LongWritable binIndex;
        private TemporalBin temporalBin;
        private boolean mustRead;
        private boolean lastItemValid;
        private IOException ioException;

        SequenceFileBinIterator(SequenceFile.Reader reader) {
            this.reader = reader;
            mustRead = true;
            lastItemValid = true;
        }

        public IOException getIOException() {
            return ioException;
        }

        @Override
        public boolean hasNext() {
            maybeReadNext();
            return lastItemValid;
        }

        @Override
        public TemporalBin next() {
            maybeReadNext();
            if (!lastItemValid) {
                throw new NoSuchElementException();
            }
            mustRead = true;
            return temporalBin;
        }

        private void maybeReadNext() {
            if (mustRead && lastItemValid) {
                mustRead = false;
                try {
                    binIndex = new LongWritable();
                    temporalBin = new TemporalBin();
                    lastItemValid = reader.next(binIndex, temporalBin);
                    if (lastItemValid) {
                        temporalBin.setIndex(binIndex.get());
                    }
                } catch (IOException e) {
                    ioException = e;
                    throw new IllegalStateException(e);
                }
            }
        }

        @Override
        public void remove() {
            throw new IllegalStateException("remove() not supported");
        }
    }

}
