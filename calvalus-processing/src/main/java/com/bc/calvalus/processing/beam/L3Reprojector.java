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

package com.bc.calvalus.processing.beam;

import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.binning.BinningContext;
import com.bc.calvalus.binning.BinningGrid;
import com.bc.calvalus.binning.TemporalBin;
import com.bc.calvalus.binning.WritableVector;
import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
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

    static void reprojectPart(BinningContext binningContext,
                              Rectangle pixelRegion,
                              SequenceFile.Reader temporalBinReader,
                              TemporalBinProcessor temporalBinProcessor) throws Exception {
        final int y1 = pixelRegion.y;
        final int y2 = pixelRegion.y + pixelRegion.height - 1;
        final BinningGrid binningGrid = binningContext.getBinningGrid();
        final int gridWidth = binningGrid.getNumRows() * 2;
        final int gridHeight = binningGrid.getNumRows();

        int lastRowIndex = -1;
        final ArrayList<TemporalBin> binRow = new ArrayList<TemporalBin>();
        while (true) {
            IntWritable binIndex = new IntWritable();
            TemporalBin temporalBin = new TemporalBin();
            if (!temporalBinReader.next(binIndex, temporalBin)) {
                if (lastRowIndex >= y1 && lastRowIndex <= y2) {
                    // last row
                    reprojectRow(binningContext,
                                 pixelRegion, lastRowIndex, binRow,
                                 temporalBinProcessor,
                                 gridWidth, gridHeight);
                }
                binRow.clear();
                break;
            }
            int rowIndex = binningGrid.getRowIndex(binIndex.get());
            if (rowIndex != lastRowIndex) {
                if (lastRowIndex >= y1 && lastRowIndex <= y2) {
                    reprojectRow(binningContext,
                                 pixelRegion, lastRowIndex, binRow,
                                 temporalBinProcessor,
                                 gridWidth, gridHeight);
                }
                binRow.clear();
                lastRowIndex = rowIndex;
            }
            temporalBin.setIndex(binIndex.get());
            binRow.add(temporalBin);
        }
    }

    // todo - use instead the following method, it can be tested! (nf)
    static void reprojectPart(BinningContext binningContext,
                              Rectangle pixelRegion,
                              Iterator<TemporalBin> temporalBins,
                              TemporalBinProcessor temporalBinProcessor) throws Exception {
        final int y1 = pixelRegion.y;
        final int y2 = pixelRegion.y + pixelRegion.height - 1;
        final BinningGrid binningGrid = binningContext.getBinningGrid();
        final int gridWidth = binningGrid.getNumRows() * 2;
        final int gridHeight = binningGrid.getNumRows();

        int lastRowIndex = -1;
        final ArrayList<TemporalBin> binRow = new ArrayList<TemporalBin>();
        while (temporalBins.hasNext()) {
            TemporalBin temporalBin = temporalBins.next();
            int rowIndex = binningGrid.getRowIndex(temporalBin.getIndex());
            if (rowIndex != lastRowIndex) {
                if (lastRowIndex >= y1 && lastRowIndex <= y2) {
                    reprojectRow(binningContext,
                                 pixelRegion, lastRowIndex, binRow,
                                 temporalBinProcessor,
                                 gridWidth, gridHeight);
                }
                binRow.clear();
                lastRowIndex = rowIndex;
            }
            binRow.add(temporalBin);
        }

        if (lastRowIndex >= y1 && lastRowIndex <= y2) {
            // last row
            reprojectRow(binningContext,
                         pixelRegion, lastRowIndex, binRow,
                         temporalBinProcessor,
                         gridWidth, gridHeight);
        }
    }

    static final class TemporalBinIterator implements Iterator<TemporalBin> {
        private final SequenceFile.Reader reader;
        private IntWritable binIndex;
        private TemporalBin temporalBin;
        private boolean mustRead;
        private boolean lastItemValid;
        private IOException ioException;

        TemporalBinIterator(SequenceFile.Reader reader) {
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
                    binIndex = new IntWritable();
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

    static void reprojectRow(BinningContext ctx,
                             Rectangle pixelRegion,
                             int y,
                             List<TemporalBin> binRow,
                             TemporalBinProcessor temporalBinProcessor,
                             int gridWidth,
                             int gridHeight) throws Exception {
        final int x1 = pixelRegion.x;
        final int x2 = pixelRegion.x + pixelRegion.width - 1;
        final int y1 = pixelRegion.y;
        if (binRow.isEmpty()) {
            for (int x = x1; x <= x2; x++) {
                temporalBinProcessor.processMissingBin(x - x1, y - y1);
            }
            return;
        }
        final BinningGrid binningGrid = ctx.getBinningGrid();
        final BinManager binManager = ctx.getBinManager();
        final WritableVector outputVector = binManager.createOutputVector();
        final double lat = 90.0 - (y + 0.5) * 180.0 / gridHeight;
        int lastBinIndex = -1;
        TemporalBin temporalBin = null;
        int rowIndex = -1;
        for (int x = x1; x <= x2; x++) {
            double lon = -180.0 + (x + 0.5) * 360.0 / gridWidth;
            int wantedBinIndex = binningGrid.getBinIndex(lat, lon);
            if (lastBinIndex != wantedBinIndex) {
                // search temporalBin for wantedBinIndex
                temporalBin = null;
                for (int i = rowIndex + 1; i < binRow.size(); i++) {
                    final int binIndex = binRow.get(i).getIndex();
                    if (binIndex == wantedBinIndex) {
                        temporalBin = binRow.get(i);
                        binManager.computeOutput(temporalBin, outputVector);
                        lastBinIndex = wantedBinIndex;
                        rowIndex = i;
                        break;
                    } else if (binIndex > wantedBinIndex) {
                        break;
                    }
                }
            }
            if (temporalBin != null) {
                temporalBinProcessor.processBin(x - x1, y - y1, temporalBin, outputVector);
            } else {
                temporalBinProcessor.processMissingBin(x - x1, y - y1);
            }
        }
    }

    /**
     * Processes temporal bins.
     */
    public static abstract class TemporalBinProcessor {
        void begin(BinningContext ctx) throws Exception {
        }

        /**
         * Processes a temporal bin and its output properties.
         *
         * @param x            current pixel X coordinate
         * @param y            current pixel Y coordinate
         * @param temporalBin  the current temporal bin
         * @param outputVector the current output vector
         * @throws Exception if an error occurred
         */
        public abstract void processBin(int x, int y, TemporalBin temporalBin, WritableVector outputVector) throws Exception;

        /**
         * Processes a missing bin.
         *
         * @param x current pixel X coordinate
         * @param y current pixel Y coordinate
         * @throws Exception if an error occurred
         */
        public abstract void processMissingBin(int x, int y) throws Exception;

        public void end(BinningContext ctx) throws Exception {
        }
    }

}
