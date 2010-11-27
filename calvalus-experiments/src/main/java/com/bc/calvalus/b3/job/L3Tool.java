package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.BinManager;
import com.bc.calvalus.b3.BinningContext;
import com.bc.calvalus.b3.BinningGrid;
import com.bc.calvalus.b3.IsinBinningGrid;
import com.bc.calvalus.b3.SpatialBin;
import com.bc.calvalus.b3.TemporalBin;
import com.bc.calvalus.b3.WritableVector;
import com.bc.calvalus.experiments.processing.N1InputFormat;
import com.bc.calvalus.experiments.util.Args;
import com.bc.calvalus.experiments.util.CalvalusLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import static com.bc.calvalus.b3.job.L3Config.*;

/**
 * Creates and runs Hadoop job for L3 processing.
 *
 * @author Norman Fomferra
 * @author Marco Zuehlke
 */
public class L3Tool extends Configured implements Tool {

    static final Logger LOG = CalvalusLogger.getLogger();

    static final String MERIS_INPUT_MONTH_DIR = "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/2008/06/%02d";

    @Override
    public int run(String[] args) throws Exception {

        try {
            // parse command line arguments
            Args options = new Args(args);
            String destination = options.getArgs()[0];
            LOG.info(MessageFormat.format("start L3 processing to {0}", destination));
            long startTime = System.nanoTime();

            // construct job and set parameters and handlers
            Job job = new Job(getConf());
            job.setJarByClass(getClass());

            Configuration configuration = job.getConfiguration();
            configuration.setInt("io.file.buffer.size", 1024 * 1024); // default is 4096

            configuration.set("mapred.map.tasks.speculative.execution", "false");
            configuration.set("mapred.reduce.tasks.speculative.execution", "false");
            // disable reuse for now....
            // job.getConfiguration().set("mapred.job.reuse.jvm.num.tasks", "-1");
            configuration.set("mapred.child.java.opts", "-Xmx1024m");
            int numReducers = 10;
            job.setNumReduceTasks(numReducers);

            if (configuration.get(CONFNAME_L3_NUM_SCANS_PER_SLICE) == null) {
                configuration.setInt(CONFNAME_L3_NUM_SCANS_PER_SLICE, L3Config.DEFAULT_L3_NUM_SCANS_PER_SLICE);
            }
            if (configuration.get(CONFNAME_L3_GRID_NUM_ROWS) == null) {
                configuration.setInt(CONFNAME_L3_GRID_NUM_ROWS, IsinBinningGrid.DEFAULT_NUM_ROWS);
            }
            if (configuration.get(CONFNAME_L3_NUM_DAYS) == null) {
                configuration.setInt(CONFNAME_L3_NUM_DAYS, L3Config.DEFAULT_L3_NUM_NUM_DAYS);
            }
            configuration.set(CONFNAME_L3_MASK_EXPR, "!l1_flags.INVALID && !l1_flags.BRIGHT && l1_flags.LAND_OCEAN");
            configuration.set(String.format(CONFNAME_L3_VARIABLES_i_NAME, 0), "ndvi");
            configuration.set(String.format(CONFNAME_L3_VARIABLES_i_EXPR, 0), "(radiance_10 - radiance_6) / (radiance_10 + radiance_6)");
            configuration.set(String.format(CONFNAME_L3_AGG_i_TYPE, 0), "AVG");
            configuration.set(String.format(CONFNAME_L3_AGG_i_VAR_NAME, 0), "ndvi");
            configuration.set(String.format(CONFNAME_L3_AGG_i_WEIGHT_COEFF, 0), "0.5");

            job.setJobName(String.format("l3_ndvi_%dd_%dr",
                                         configuration.getInt(CONFNAME_L3_NUM_DAYS, -1),
                                         configuration.getInt(CONFNAME_L3_GRID_NUM_ROWS, -1)));

            job.setInputFormatClass(N1InputFormat.class);
            configuration.setInt(N1InputFormat.NUMBER_OF_SPLITS, 1);

            job.setMapperClass(L3Mapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(SpatialBin.class);

            job.setPartitionerClass(L3Partitioner.class);

            job.setReducerClass(L3Reducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(TemporalBin.class);

            // job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // provide input and output directories to job
            for (int day = 1; day <= 16; day++) {
                String pathName = String.format(MERIS_INPUT_MONTH_DIR, day);
                FileInputFormat.addInputPath(job, new Path(pathName));
            }
            Path output = new Path(destination);
            FileOutputFormat.setOutputPath(job, output);

            int result = 0;
            result = job.waitForCompletion(true) ? 0 : 1;
            long stopTime = System.nanoTime();
            LOG.info(MessageFormat.format("stop L3 processing after {0} sec", (stopTime - startTime) / 1E9));

            processL3Output(configuration, output, job.getNumReduceTasks());

            return result;

        } catch (Exception ex) {

            System.err.println("failed: " + ex.getMessage());
            ex.printStackTrace(System.err);
            return 1;

        }

    }

    private void processL3Output(Configuration configuration, Path output, int numParts) throws IOException {
        final BinningContext ctx = L3Config.getBinningContext(configuration);

        LOG.info(MessageFormat.format("start reprojection, collecting {0} parts", numParts));
        BinningGrid binningGrid = ctx.getBinningGrid();
        int width = binningGrid.getNumRows() * 2;
        int height = binningGrid.getNumRows();
        float[] nobsData = new float[width * height];
        float[] meanData = new float[width * height];
        float[] sigmaData = new float[width * height];
        Arrays.fill(nobsData, Float.NaN);
        Arrays.fill(meanData, Float.NaN);
        Arrays.fill(sigmaData, Float.NaN);

        int numObsMaxTotal = -1;
        int numPassesMaxTotal = -1;

        long startTime = System.nanoTime();

        for (int i = 0; i < numParts; i++) {
            Path partFile = new Path(output, String.format("part-r-%05d", i));
            SequenceFile.Reader reader = new SequenceFile.Reader(partFile.getFileSystem(configuration), partFile, configuration);

            int numObsMaxPart = -1;
            int numPassesMaxPart = -1;

            LOG.info(MessageFormat.format("reading part {0}", partFile));

            try {
                int lastRowIndex = -1;
                ArrayList<TemporalBin> binRow = new ArrayList<TemporalBin>();
                while (true) {
                    IntWritable binIndex = new IntWritable();
                    TemporalBin temporalBin = new TemporalBin();
                    if (!reader.next(binIndex, temporalBin)) {
                        // last row
                        processBinRow(ctx,
                                      lastRowIndex, binRow,
                                      nobsData, meanData, sigmaData,
                                      width, height);
                        binRow.clear();
                        break;
                    }
                    int rowIndex = binningGrid.getRowIndex(binIndex.get());
                    if (rowIndex != lastRowIndex) {
                        processBinRow(ctx,
                                      lastRowIndex, binRow,
                                      nobsData, meanData, sigmaData,
                                      width, height);
                        binRow.clear();
                        lastRowIndex = rowIndex;
                    }
                    temporalBin.setIndex(binIndex.get());
                    binRow.add(temporalBin);

                    numObsMaxPart = Math.max(numObsMaxPart, temporalBin.getNumObs());
                    numPassesMaxPart = Math.max(numPassesMaxPart, temporalBin.getNumPasses());
                }
            } finally {
                reader.close();
            }

            LOG.info(MessageFormat.format("numObsMaxPart = {0}, numPassesMaxPart = {1}", numObsMaxPart, numPassesMaxPart));

            numObsMaxTotal = Math.max(numObsMaxTotal, numObsMaxPart);
            numPassesMaxTotal = Math.max(numPassesMaxTotal, numPassesMaxPart);
        }
        long stopTime = System.nanoTime();

        LOG.info(MessageFormat.format("numObsMaxTotal = {0}, numPassesMaxTotal = {1}", numObsMaxTotal, numPassesMaxTotal));
        LOG.info(MessageFormat.format("stop reprojection after {0} sec", (stopTime - startTime) / 1E9));

        String baseName = String.format("l3_ndvi_%dd_%dr", configuration.getInt(CONFNAME_L3_NUM_DAYS, -1),
                                        configuration.getInt(CONFNAME_L3_GRID_NUM_ROWS, -1));
        writeImage(width, height, nobsData, 0.5f, new File(baseName + "_nobs.png"));
        writeImage(width, height, meanData, 255 / 0.8f, new File(baseName + "_mean.png"));
        writeImage(width, height, sigmaData, 255 / 0.1f, new File(baseName + "_sigma.png"));
    }

    private void writeImage(int width, int height, float[] rawData, float factor, File outputImageFile) throws IOException {
        LOG.info(MessageFormat.format("writing image {0}", outputImageFile));
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);
        DataBufferByte dataBuffer = (DataBufferByte) image.getRaster().getDataBuffer();
        byte[] data = dataBuffer.getData();
        for (int i = 0; i < rawData.length; i++) {
            int sample = (int) (factor * rawData[i]);
            if (sample < 0) {
                sample = 0;
            } else if (sample > 255) {
                sample = 255;
            }
            data[i] = (byte) sample;
        }
        ImageIO.write(image, "PNG", outputImageFile);
    }

    static void processBinRow(BinningContext ctx,
                              int y, List<TemporalBin> binRow,
                              float[] nobsData, float[] meanData, float[] sigmaData,
                              int width, int height) {
        if (y >= 0 && !binRow.isEmpty()) {
//            LOG.info("row " + y + ": processing " + binRow.size() + " bins, bin #0 = " + binRow.get(0));
            processBinRow0(ctx,
                           y, binRow,
                           nobsData, meanData, sigmaData,
                           width, height);
        } else {
//            LOG.info("row " + y + ": no bins");
        }
    }

    static void processBinRow0(BinningContext ctx,
                               int y,
                               List<TemporalBin> binRow,
                               float[] nobsData,
                               float[] meanData,
                               float[] sigmaData,
                               int width,
                               int height) {
        final BinningGrid binningGrid = ctx.getBinningGrid();
        final BinManager binManager = ctx.getBinManager();
        final WritableVector outputVector = binManager.createOutputVector();
        final int offset = y * width;
        final double lat = -90.0 + (y + 0.5) * 180.0 / height;
        int lastBinIndex = -1;
        TemporalBin temporalBin = null;
        int rowIndex = -1;
        float lastMean = Float.NaN;
        float lastSigma = Float.NaN;
        for (int x = 0; x < width; x++) {
            double lon = -180.0 + (x + 0.5) * 360.0 / width;
            int wantedBinIndex = binningGrid.getBinIndex(lat, lon);
            if (lastBinIndex != wantedBinIndex) {
                //search
                temporalBin = null;
                for (int i = rowIndex + 1; i < binRow.size(); i++) {
                    final int binIndex = binRow.get(i).getIndex();
                    if (binIndex == wantedBinIndex) {
                        temporalBin = binRow.get(i);
                        binManager.computeOutput(temporalBin, outputVector);
                        lastMean = outputVector.get(0);
                        lastSigma = outputVector.get(1);
                        lastBinIndex = wantedBinIndex;
                        rowIndex = i;
                        break;
                    } else if (binIndex > wantedBinIndex) {
                        break;
                    }
                }
            }
            if (temporalBin != null) {
                nobsData[offset + x] = temporalBin.getNumObs();
                meanData[offset + x] = lastMean;
                sigmaData[offset + x] = lastSigma;
            } else {
                nobsData[offset + x] = Float.NaN;
                meanData[offset + x] = Float.NaN;
                sigmaData[offset + x] = Float.NaN;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new L3Tool(), args));
    }
}
