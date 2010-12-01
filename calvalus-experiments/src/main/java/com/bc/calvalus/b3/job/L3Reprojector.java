package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.BinManager;
import com.bc.calvalus.b3.BinningContext;
import com.bc.calvalus.b3.BinningGrid;
import com.bc.calvalus.b3.TemporalBin;
import com.bc.calvalus.b3.WritableVector;
import com.bc.calvalus.experiments.util.CalvalusLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * Reprojects the output of the {@code L3Tool} (reducer parts stored as sequence files).
 *
 * @author Norman Fomferra
 */
public class L3Reprojector {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String PART_R = "part-r-";

    // todo - no good: TemporalBinProcessor must know that width=binningGrid.getNumRows()*2 and height=binningGrid.getNumRows()

    public static void reproject(BinningContext ctx,
                                 Path output,
                                 Configuration conf,
                                 TemporalBinProcessor temporalBinProcessor) throws Exception {
        BinningGrid binningGrid = ctx.getBinningGrid();
        int width = binningGrid.getNumRows() * 2;
        int height = binningGrid.getNumRows();

        long startTime = System.nanoTime();

        final FileSystem hdfs = output.getFileSystem(conf);
        final FileStatus[] fileStati = hdfs.listStatus(output, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(PART_R);
            }
        });

        LOG.info(MessageFormat.format("start reprojection, collecting {0} parts", fileStati.length));

        Arrays.sort(fileStati);

        temporalBinProcessor.begin(ctx);
        for (FileStatus fileStatus : fileStati) {
            Path partFile = fileStatus.getPath();
            SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, partFile, conf);

            LOG.info(MessageFormat.format("reading and reprojecting part {0}", partFile));

            try {
                int lastRowIndex = -1;
                ArrayList<TemporalBin> binRow = new ArrayList<TemporalBin>();
                while (true) {
                    IntWritable binIndex = new IntWritable();
                    TemporalBin temporalBin = new TemporalBin();
                    if (!reader.next(binIndex, temporalBin)) {
                        // last row
                        reprojectRow(ctx,
                                     lastRowIndex, binRow,
                                     temporalBinProcessor,
                                     width, height);
                        break;
                    }
                    int rowIndex = binningGrid.getRowIndex(binIndex.get());
                    if (rowIndex != lastRowIndex) {
                        reprojectRow(ctx,
                                     lastRowIndex, binRow,
                                     temporalBinProcessor,
                                     width, height);
                        lastRowIndex = rowIndex;
                    }
                    temporalBin.setIndex(binIndex.get());
                    binRow.add(temporalBin);
                }
            } finally {
                reader.close();
            }
        }
        temporalBinProcessor.end(ctx);

        long stopTime = System.nanoTime();
        LOG.info(MessageFormat.format("stop reprojection after {0} sec", (stopTime - startTime) / 1E9));
    }


    static void reprojectRow(BinningContext ctx,
                             int y,
                             List<TemporalBin> binRow,
                             TemporalBinProcessor temporalBinProcessor,
                             int width,
                             int height) throws Exception {
        if (y < 0 || binRow.isEmpty()) {
            return;
        }
        final BinningGrid binningGrid = ctx.getBinningGrid();
        final BinManager binManager = ctx.getBinManager();
        final WritableVector outputVector = binManager.createOutputVector();
        final double lat = -90.0 + (y + 0.5) * 180.0 / height;
        int lastBinIndex = -1;
        TemporalBin temporalBin = null;
        int rowIndex = -1;
        for (int x = 0; x < width; x++) {
            double lon = -180.0 + (x + 0.5) * 360.0 / width;
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
                temporalBinProcessor.processBin(x, y, temporalBin, outputVector);
            } else {
                temporalBinProcessor.processBin(x, y, null, null);
            }
        }
        binRow.clear();
    }

    public static abstract class TemporalBinProcessor {
        void begin(BinningContext ctx) throws Exception {
        }

        /**
         *
         * @param x current pixel X coordinate
         * @param y current pixel Y coordinate
         * @param temporalBin the current temporal bin, will be null, if missing
         * @param outputVector the current output vector, will be null, if missing
         * @throws Exception if an error occured
         */
        public abstract void processBin(int x, int y, TemporalBin temporalBin, WritableVector outputVector) throws Exception;


        public void end(BinningContext ctx) throws Exception {
        }
    }

}
