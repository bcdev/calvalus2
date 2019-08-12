package com.bc.calvalus.processing.ra.stat;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.ra.RAConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * calculates statistics for multiple bands
 */
public class RegionAnalysis {

    private static final Logger LOG = CalvalusLogger.getLogger();

    private final RADateRanges dateRanges;
    private final HandleAll dataRangeHandler;
    private final HandleAll regionHandler;
    private final Statistics[] stats;
    private final StatisticsWriter statisticsWriter;
    private final List<String> regionNameList;
    private final boolean withProductNames;

    private String currentRegionName;
    private long currentTime;
    private int numPasses;
    private long numObs;
    private String productName = null;

    public RegionAnalysis(RADateRanges dateRanges, RAConfig raConfig, boolean binValuesAsRatio, WriterFactory writerFactor) throws IOException, InterruptedException {
        this.dateRanges = dateRanges;
        this.dataRangeHandler = new HandleAll(dateRanges.numRanges());
        String[] internalRegionNames = raConfig.getInternalRegionNames();
        this.regionNameList = Arrays.asList(internalRegionNames);
        this.regionHandler = new HandleAll(internalRegionNames.length);

        RAConfig.BandConfig[] bandConfigs = raConfig.getBandConfigs();
        stats = new Statistics[bandConfigs.length];
        for (int i = 0; i < bandConfigs.length; i++) {
            RAConfig.BandConfig bConfig = bandConfigs[i];
            stats[i] = new Statistics(bConfig.getNumBins(), bConfig.getMin(), bConfig.getMax(), raConfig.getPercentiles(), binValuesAsRatio);
        }
        withProductNames = raConfig.withProductNames();
        statisticsWriter = new StatisticsWriter(raConfig, stats, writerFactor);
    }

    public void addData(long time, int numObs, float[][] samples, String... productNames) throws IOException {
        int newDateRange = dateRanges.findIndex(time);
        if (newDateRange == -1) {
            String out_ouf_range_date = dateRanges.format(time);
            LOG.warning("out_ouf_range_date = " + out_ouf_range_date + " --> ignoring extract data");
        } else {
            if (newDateRange != dataRangeHandler.current()) {
                writeCurrentRecord();
                resetRecord();
                writeEmptyRecords(regionHandler.current(), dataRangeHandler.preceedingUnhandledIndices(newDateRange));
            }
            accumulate(time, numObs, samples);
            productName = productNames.length > 0 ? productNames[0] : null;
        }
    }

    public void startRegion(int regionId, String regionName) throws IOException {
        dataRangeHandler.reset();
        //for (int regionIndex : regionHandler.preceedingUnhandledIndices(regionNameList.indexOf(regionName))) {
        for (int regionIndex : regionHandler.preceedingUnhandledIndices(regionId)) {
            this.currentRegionName = regionNameList.get(regionIndex);
            statisticsWriter.createWriters(currentRegionName);
            writeEmptyRecords(regionIndex, dataRangeHandler.remaining());
            statisticsWriter.closeWriters(currentRegionName);
        }
        this.currentRegionName = regionName;
        statisticsWriter.createWriters(currentRegionName);
    }

    public void endRegion() throws IOException {
        writeCurrentRecord();
        resetRecord();
        writeEmptyRecords(regionHandler.current(), dataRangeHandler.remaining());
        statisticsWriter.closeWriters(currentRegionName);
    }

    public void close() throws IOException {
        dataRangeHandler.reset();
        for (int regionIndex : regionHandler.remaining()) {
            this.currentRegionName = regionNameList.get(regionIndex);
            statisticsWriter.createWriters(currentRegionName);
            writeEmptyRecords(regionIndex, dataRangeHandler.remaining());
            statisticsWriter.closeWriters(currentRegionName);
        }
        statisticsWriter.close();
    }

    /////////////////////////////////

    private void accumulate(long time, int numObs, float[][] samples) {
        if (time != currentTime) {
            currentTime = time;
            numPasses++;
        }
        this.numObs += numObs;
        if (samples.length != stats.length) {
            throw new IllegalArgumentException(String.format("samples.length(%d) does not match num bands(%d)", samples.length, stats.length));
        }
        for (int bandId = 0; bandId < samples.length; bandId++) {
            stats[bandId].process(samples[bandId]);
        }
    }

    private void resetRecord() {
        currentTime = -1;
        numObs = 0;
        numPasses = 0;
        for (Statistics stat : stats) {
            stat.reset();
        }
    }

    private void writeCurrentRecord() throws IOException {
        if (dataRangeHandler.current() > -1) {
            writeRecord(regionHandler.current(), dataRangeHandler.current());
        }
    }

    private void writeEmptyRecords(int regionIndex, int[] dateIndices) throws IOException {
        for (int dateIndex : dateIndices) {
            writeRecord(regionIndex, dateIndex);
        }
    }

    private void writeRecord(int regionIndex, int dateIndex) throws IOException {
        String dStart = dateRanges.formatStart(dateIndex);
        String dEnd = dateRanges.formatEnd(dateIndex);
        List<String> commonStats = getCommonStats(dStart, dEnd, numPasses, numObs);

        statisticsWriter.writeRecord(regionIndex, commonStats);
    }

    private List<String> getCommonStats(String dStart, String dEnd, int numPasses, long numObs) {
        List<String> records = new ArrayList<>();
        records.add(currentRegionName);
        records.add(dStart);
        records.add(dEnd);
        if (withProductNames){
            records.add(productName);
        }
        records.add(Integer.toString(numPasses));
        records.add(Long.toString(numObs));
        return records;
    }
}
