package com.bc.calvalus.processing.ra.stat;

import com.bc.calvalus.processing.ra.RAConfig;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * calculates statistics for multiple bands
 */
public abstract class RegionAnalysis {

    private final RADateRanges dateRanges;

    private final Statistics[] stats;
    private final Writer analysisWriter;
    private final Writer[] histogramWriters;

    private String regionName;
    private int numPasses;
    private int numObs;
    private int currentDateRange;

    protected RegionAnalysis(RADateRanges dateRanges, RAConfig.BandConfig[] bandConfigs) throws IOException, InterruptedException {
        this.dateRanges = dateRanges;

        stats = new Statistics[bandConfigs.length];
        String[] bandNames = new String[bandConfigs.length];
        for (int i = 0; i < bandConfigs.length; i++) {
            RAConfig.BandConfig bConfig = bandConfigs[i];
            bandNames[i] = bConfig.getName();
            stats[i] = new Statistics(bConfig.getNumBins(), bConfig.getLowValue(), bConfig.getHighValue());
        }
        analysisWriter = createWriter("region-analysis.csv");
        writeLine(analysisWriter, getHeader(bandNames, true));

        histogramWriters = new Writer[bandConfigs.length];
        for (int i = 0; i < bandConfigs.length; i++) {
            histogramWriters[i] = createWriter("region-histogram-" + bandNames[i] + ".csv");
            writeLine(histogramWriters[i], getHeader(bandNames, false));
        }
    }

    public abstract Writer createWriter(String fileName) throws IOException, InterruptedException;

    public void addData(long time, int numPixel, float[][] samples) throws IOException {
        int newDateRange = dateRanges.findIndex(time);
        if (newDateRange == -1) {
            String out_ouf_range_date = dateRanges.format(time);
            System.out.println("out_ouf_range_date = " + out_ouf_range_date + " --> ignoring extract data");
        } else {
            if (newDateRange != currentDateRange) {
                writeCurrentRecord();
                resetRecord();
                writeEmptyRecords(currentDateRange + 1, newDateRange);
                currentDateRange = newDateRange;
            }
            accumulate(numPixel, samples);
        }
    }

    public void startRegion(String regionName) {
        this.regionName = regionName;
        this.currentDateRange = -1;
    }

    public void endRegion() throws IOException {
        writeCurrentRecord();
        resetRecord();
        writeEmptyRecords(currentDateRange + 1, dateRanges.numRanges());
    }

    public void close() throws IOException {
        analysisWriter.close();
        for (Writer writer : histogramWriters) {
            writer.close();
        }
    }

    /////////////////////////////////

    private void accumulate(int numPixel, float[][] samples) {
        numPasses++;
        numObs += numPixel;
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
        if (currentDateRange > -1) {
            writeRecord(currentDateRange);
        }
    }

    private void writeEmptyRecords(int beginIndex, int endIndex) throws IOException {
        for (int rowIndex = beginIndex; rowIndex < endIndex; rowIndex++) {
            writeRecord(rowIndex);
        }
    }

    private void writeRecord(int rowIndex) throws IOException {
        String dStart = dateRanges.formatStart(rowIndex);
        String dEnd = dateRanges.formatEnd(rowIndex);
        List<String> commonStats = getStats(dStart, dEnd, numPasses, numObs);

        List<String> records = new ArrayList<>();
        records.addAll(commonStats);
        for (Statistics stat : stats) {
            records.addAll(stat.getStatisticsRecords());
        }
        writeLine(analysisWriter, records);

        for (int bandIndex = 0; bandIndex < this.stats.length; bandIndex++) {
            records = new ArrayList<>(commonStats);
            records.addAll(stats[bandIndex].getHistogramRecords());
            writeLine(histogramWriters[bandIndex], records);
        }
    }

    private List<String> getHeader(String[] bandNames, boolean stat) {
        List<String> records = new ArrayList<>();
        records.add("RegionId");
        records.add("TimeWindow_start");
        records.add("TimeWindow_end");
        records.add("numPasses");
        records.add("numObs");
        for (int bandIndex = 0; bandIndex < bandNames.length; bandIndex++) {
            if (stat) {
                records.addAll(stats[bandIndex].getStatisticsHeaders(bandNames[bandIndex]));
            } else {
                records.addAll(stats[bandIndex].getHistogramHeaders());
            }
        }
        return records;
    }

    private List<String> getStats(String dStart, String dEnd, int numPasses, int numObs) {
        List<String> records = new ArrayList<>();
        records.add(regionName);
        records.add(dStart);
        records.add(dEnd);
        records.add(Integer.toString(numPasses));
        records.add(Integer.toString(numObs));
        return records;
    }

    private static void writeLine(Writer writer, List<String> fields) throws IOException {
        writer.write(String.join("\t", fields) + "\n");
    }
}
