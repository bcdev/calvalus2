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

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.l3.HadoopBinManager;
import com.bc.calvalus.processing.l3.L3TemporalBin;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.binning.Aggregator;
import org.esa.snap.binning.BinManager;
import org.esa.snap.binning.BinningContext;
import org.esa.snap.binning.operator.BinningConfig;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Aggregates temporal bins of a region for all time intervals and all bin cells.
 * The bins come ordered by time and by bin index.
 * In order to get the columns correct - one for each bin index - we memorise the
 * mapping from bin index to column and maintain it in the following time steps.
 *
 * @author Norman Fomferra
 * @author Martin
 */
public class TAReducer extends Reducer<TAKey, L3TemporalBinWithIndex, Text, TAPoint> implements Configurable {

    public static final String DATE_PATTERN = "yyyy-MM-dd";

    public static final Logger LOGGER = CalvalusLogger.getLogger();

    private Configuration conf;
    BinManager binManager;
    String minDate;
    String maxDate;
    private int compositingPeriodLength;
    private List<String> outputFeatureNames;
    private TAConfig.RegionConfiguration[] regions = null;
    private String outputDirPath;

    @Override
    protected void reduce(TAKey taKey, Iterable<L3TemporalBinWithIndex> bins, Context context) throws IOException, InterruptedException {

        final DateFormat dateFormat = DateUtils.createDateFormat(DATE_PATTERN);
        LOGGER.info("handling bins for " + taKey + " ...");

        // this is for the CSV tables that go into files per feature + numObs
        final TAConfig.RegionConfiguration region = regions[taKey.getRegionId()];
        FileSystem fs = new Path(outputDirPath).getFileSystem(conf);
        int numFeatures = outputFeatureNames.size();
        PrintWriter[] writers = new PrintWriter[numFeatures + 1];
        StringBuffer[] lines = new StringBuffer[numFeatures + 1];
        ArrayList<float[]> lineValuesList = new ArrayList<>();
        Map<Long,Integer> columnOfBinIndexMap = new HashMap<>();

        // this is for aggregation and goes into the sequence file
        L3TemporalBin outputBin = null;

        long currentTime = -1;

        // we have to do all in a single loop to read each temporal bin only once
        for (L3TemporalBinWithIndex bin : bins) {
            if (currentTime == -1) {
                // first line starts
                currentTime = bin.getTime();
                LOGGER.info("Handling first time step " + dateFormat.format(new Date(currentTime)));
                // initialise aggregation
                outputBin = (L3TemporalBin) binManager.createTemporalBin(-1);
                // provide files and line container for features and for numObs
                initialiseCsvWriting(fs, region, numFeatures, writers, lines);
            } else if (bin.getTime() != currentTime) {
                // next line starts
                writeAggregatedRecord(context, region, dateFormat, currentTime, outputBin);
                outputBin = (L3TemporalBin) binManager.createTemporalBin(-1);
                // write previous line
                final String timeString = dateFormat.format(new Date(currentTime));
                writeCurrentLines(timeString, numFeatures, writers, lineValuesList);
                // switch to next line rsp. time
                currentTime = bin.getTime();
                LOGGER.info("Handling time step " + dateFormat.format(new Date(currentTime)));
            }
            final float[] values = getValueContainerForBinIndex(columnOfBinIndexMap, lineValuesList, numFeatures, bin.getIndex());
            for (int i = 0; i < numFeatures; ++i) {
                values[i] = bin.getFeatureValues()[i];
            }
            values[numFeatures] = bin.getNumObs();
            // aggregate (spatially in fact)
            binManager.aggregateTemporalBin(bin, outputBin);
        }
        // flush if we have read anything at all
        writeCurrentLines(dateFormat.format(currentTime), numFeatures, writers, lineValuesList);
        if (currentTime != -1) {
            writeAggregatedRecord(context, region, dateFormat, currentTime, outputBin);
            for (int i = 0; i < numFeatures + 1; ++i) {
                writers[i].flush();
                writers[i].close();
            }
        }
    }

    private float[] getValueContainerForBinIndex(Map<Long, Integer> columnOfBinIndexMap, ArrayList<float[]> lineValuesList, int numFeatures, long binIndex) {
        final float[] values;
        final Integer column = columnOfBinIndexMap.get(binIndex);
        if (column != null) {
            values = lineValuesList.get(column);
        } else {
            values = new float[numFeatures + 1];
            columnOfBinIndexMap.put(binIndex, lineValuesList.size());
            lineValuesList.add(values);
        }
        return values;
    }

    private void writeAggregatedRecord(Context context, TAConfig.RegionConfiguration region, DateFormat dateFormat, long currentTime, L3TemporalBin outputBin) throws IOException, InterruptedException {
        String startOfPeriod = dateFormat.format(new Date(currentTime - Math.abs(compositingPeriodLength) * 86400000L / 2));
        String endOfPeriod = dateFormat.format(new Date(currentTime + Math.abs(compositingPeriodLength) * 86400000L / 2));
        context.write(new Text(region.getName() + "-" + dateFormat.format(new Date(currentTime))),
                      new TAPoint(region.getName(), startOfPeriod, endOfPeriod, outputBin));
        LOGGER.info("Writing aggregated values for " + region.getName() + " at " + startOfPeriod);
    }

    private void writeCurrentLines(String timeString, int numFeatures, PrintWriter[] writers, ArrayList<float[]> lineValues) {
        for (int i = 0; i < numFeatures + 1; ++i) {
            writers[i].print(timeString);
            for (float[] values : lineValues) {
                writers[i].print("\t");
                writers[i].print(values[i]);
                values[i] = Float.NaN;
            }
            writers[i].println();
        }
    }

    private void initialiseCsvWriting(FileSystem fs, TAConfig.RegionConfiguration region, int numFeatures, PrintWriter[] writers, StringBuffer[] lines) throws IOException {
        for (int i = 0; i < numFeatures; ++i) {
            final Path path = new Path(outputDirPath + "/" + region.getName() + "-" + outputFeatureNames.get(i) + "-timeseries.csv");
            LOGGER.info("Creating output file " + path.getName());
            final FSDataOutputStream fsDataOutputStream = fs.create(path);
            writers[i] = new PrintWriter(new BufferedWriter(new OutputStreamWriter(fsDataOutputStream)));
            lines[i] = new StringBuffer();
        }
        final Path path = new Path(outputDirPath + "/" + region.getName() + "-" + "numobs" + "-timeseries.csv");
        final FSDataOutputStream fsDataOutputStream = fs.create(path);
        writers[numFeatures] = new PrintWriter(new BufferedWriter(new OutputStreamWriter(fsDataOutputStream)));
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
        BinningContext binningContext = HadoopBinManager.createBinningContext(binningConfig, null, null);
        binManager = binningContext.getBinManager();
        outputFeatureNames = new ArrayList<>();
        for (int i = 0; i < binManager.getAggregatorCount(); i++) {
            Aggregator aggregator = binManager.getAggregator(i);
            outputFeatureNames.addAll(Arrays.asList(aggregator.getOutputFeatureNames()));
        }
        int periodLength = conf.getInt("periodLength", -30);
        compositingPeriodLength = conf.getInt("compositingPeriodLength", periodLength);
        regions = TAConfig.get(conf).getRegions();
        outputDirPath = conf.get(JobConfigNames.CALVALUS_OUTPUT_DIR);
        minDate = conf.get(JobConfigNames.CALVALUS_MIN_DATE);
        maxDate = conf.get(JobConfigNames.CALVALUS_MAX_DATE);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
