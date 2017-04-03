package com.bc.calvalus.processing.ra.stat;

import com.bc.calvalus.processing.ra.RAConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * calculates statistics for multiple bands
 */
public class RegionAnalysis {

    private final RADateRanges dateRanges;
    private final HandleAll dataRangeHandler;
    private final HandleAll regionHandler;
    private final Statistics[] stats;
    private final StatisticsWriter statisticsWriter;
    private final List<String> regionNameList;

    private String currentRegionName;
    private int numPasses;
    private int numObs;

    public RegionAnalysis(RADateRanges dateRanges, RAConfig raConfig, WriterFactory writerFactor) throws IOException, InterruptedException {
        this.dateRanges = dateRanges;
        this.dataRangeHandler = new HandleAll(dateRanges.numRanges());
        String[] internalRegionNames = raConfig.getInternalRegionNames();
        this.regionNameList = Arrays.asList(internalRegionNames);
        this.regionHandler = new HandleAll(internalRegionNames.length);

        RAConfig.BandConfig[] bandConfigs = raConfig.getBandConfigs();
        stats = new Statistics[bandConfigs.length];
        for (int i = 0; i < bandConfigs.length; i++) {
            RAConfig.BandConfig bConfig = bandConfigs[i];
            stats[i] = new Statistics(bConfig.getNumBins(), bConfig.getLowValue(), bConfig.getHighValue());
        }
        statisticsWriter = new StatisticsWriter(raConfig, stats, writerFactor);
    }

    public void addData(long time, int numObs, float[][] samples) throws IOException {
        int newDateRange = dateRanges.findIndex(time);
        if (newDateRange == -1) {
            String out_ouf_range_date = dateRanges.format(time);
            System.out.println("out_ouf_range_date = " + out_ouf_range_date + " --> ignoring extract data");
        } else {
            if (newDateRange != dataRangeHandler.current()) {
                writeCurrentRecord();
                resetRecord();
                int[] indices = dataRangeHandler.next(newDateRange);
                writeEmptyRecords(indices);
            }
            accumulate(numObs, samples);
        }
    }

    public void startRegion(String regionName) throws IOException {
        dataRangeHandler.reset();
        for (int regionIndex : regionHandler.next(regionNameList.indexOf(regionName))) {
            this.currentRegionName = regionNameList.get(regionIndex);
            writeEmptyRecords(dataRangeHandler.remaining());
        }
        this.currentRegionName = regionName;
    }

    public void endRegion() throws IOException {
        writeCurrentRecord();
        resetRecord();
        int[] remaining = dataRangeHandler.remaining();
        writeEmptyRecords(remaining);
    }

    public void close() throws IOException {
        dataRangeHandler.reset();
        for (int regionIndex : regionHandler.remaining()) {
            this.currentRegionName = regionNameList.get(regionIndex);
            writeEmptyRecords(dataRangeHandler.remaining());
        }
        statisticsWriter.close();
    }

    /////////////////////////////////

    private void accumulate(int numObs, float[][] samples) {
        this.numPasses++;
        this.numObs += numObs;
        if (samples.length != stats.length) {
            throw new IllegalArgumentException("samples.length does not match num bands");
        }
        for (int bandId = 0; bandId < samples.length; bandId++) {
            stats[bandId].process(samples[bandId]);
        }
    }

    private void resetRecord() {
        numObs = 0;
        numPasses = 0;
        for (Statistics stat : stats) {
            stat.reset();
        }
    }

    private void writeCurrentRecord() throws IOException {
        if (dataRangeHandler.current() > -1) {
            writeRecord(dataRangeHandler.current());
        }
    }

    private void writeEmptyRecords(int[] indices) throws IOException {
        for (int index : indices) {
            writeRecord(index);
        }
    }

    private void writeRecord(int dateIndex) throws IOException {
        String dStart = dateRanges.formatStart(dateIndex);
        String dEnd = dateRanges.formatEnd(dateIndex);
        List<String> commonStats = getStats(dStart, dEnd, numPasses, numObs);

        statisticsWriter.writeRecord(regionHandler.current(), commonStats);
    }

    private List<String> getStats(String dStart, String dEnd, int numPasses, int numObs) {
        List<String> records = new ArrayList<>();
        records.add(currentRegionName);
        records.add(dStart);
        records.add(dEnd);
        records.add(Integer.toString(numPasses));
        records.add(Integer.toString(numObs));
        return records;
    }
}
