package com.bc.calvalus.processing.ra.stat;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * calculates statistics for multiple bands
 */
public class Statistics {

    private String regionName;
    private RADateRanges dateRanges;
    private final Accumulator[] accus;
    private final String[] bandNames;
    private final Writer writer;
    private final StatiticsComputer[] emptyStats;

    private int numPasses;
    private int numObs;

    private int currentDateRange = -1;

    public Statistics(RADateRanges dateRanges, String[] bandNames, Writer writer) throws IOException {
        this.dateRanges = dateRanges;
        this.bandNames = bandNames;
        this.accus = new Accumulator[bandNames.length];
        for (int i = 0; i < bandNames.length; i++) {
            this.accus[i] = new Accumulator(bandNames[i].trim());
        }
        this.writer = writer;
        this.emptyStats = createEmptyStats();
        writeLine(writer, getHeader());
    }

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
    }

    public void endRegion() throws IOException {
        writeCurrentRecord();
        resetRecord();
        writeEmptyRecords(currentDateRange + 1, dateRanges.numRanges());
        currentDateRange = -1;
    }

    /////////////////////////////////

    private void accumulate(int numPixel, float[][] samples) {
        numPasses++;
        numObs += numPixel;
        if (samples.length != bandNames.length) {
            throw new IllegalArgumentException("samples.length does not match num bands");
        }
        for (int bandId = 0; bandId < samples.length; bandId++) {
            accus[bandId].accumulate(samples[bandId]);
        }
    }

    private void resetRecord() {
        numObs = 0;
        numPasses = 0;
    }

    private void writeCurrentRecord() throws IOException {
        String dStart = dateRanges.formatStart(currentDateRange);
        String dEnd = dateRanges.formatEnd(currentDateRange);
        writeLine(writer, getStats(dStart, dEnd, numPasses, numObs, emptyStats));
    }

    private void writeEmptyRecords(int beginIndex, int endIndex) throws IOException {
        for (int i = beginIndex; i < endIndex; i++) {
            String dStart = dateRanges.formatStart(i);
            String dEnd = dateRanges.formatEnd(i);
            writeLine(writer, getStats(dStart, dEnd, 0, 0, compute()));
        }
    }

    private StatiticsComputer[] compute() {
        StatiticsComputer[] bandStats = new StatiticsComputer[accus.length];
        for (int i = 0; i < accus.length; i++) {
            Accumulator accu = accus[i];
            if (accu != null) {
                bandStats[i] = new StatiticsComputer(bandNames[i], accu.getValues());
            } else {
                bandStats[i] = new StatiticsComputer(bandNames[i]);
            }
        }
        return bandStats;
    }

    private StatiticsComputer[] createEmptyStats() {
        StatiticsComputer[] bandStats = new StatiticsComputer[bandNames.length];
        for (int i = 0; i < bandNames.length; i++) {
            bandStats[i] = new StatiticsComputer(bandNames[i]);
        }
        return bandStats;
    }

    private List<String> getHeader() {
        List<String> records = new ArrayList<>();
        records.add("RegionId");
        records.add("TimeWindow_start");
        records.add("TimeWindow_end");
        records.add("numPasses");
        records.add("numObs");
        for (String bandName : bandNames) {
            records.addAll(StatiticsComputer.getHeader(bandName));
        }
        return records;
    }

    private List<String> getStats(String dStart, String dEnd, int numPasses, int numObs, StatiticsComputer[] stats) {
        List<String> records = new ArrayList<>();
        records.add(regionName);
        records.add(dStart);
        records.add(dEnd);
        records.add(Integer.toString(numPasses));
        records.add(Integer.toString(numObs));
        for (StatiticsComputer stat : stats) {
            records.addAll(stat.getStats());
        }
        return records;
    }

    private static void writeLine(Writer writer, List<String> fields) throws IOException {
        writer.write(String.join("\t", fields) + "\n");
    }
}
