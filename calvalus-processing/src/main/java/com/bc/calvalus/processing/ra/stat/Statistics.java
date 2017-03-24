package com.bc.calvalus.processing.ra.stat;

import com.bc.calvalus.processing.ra.RAValue;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * calculates statistics for multiple bands
 */
public class Statistics {

    private final Accumulator[] accus;
    private final String dStart;
    private final String dEnd;
    private final String regionName;

    private StatiticsComputer[] bandStats;
    private int numPasses;
    private int numObs;
    private boolean headerWritten;

    public Statistics(String regionName, String[] bandNames, String dStart, String dEnd) {
        this.regionName = regionName;
        this.accus = new Accumulator[bandNames.length];
        this.dStart = dStart;
        this.dEnd = dEnd;
        for (int i = 0; i < bandNames.length; i++) {
            this.accus[i] = new Accumulator(bandNames[i].trim());
        }
        this.numObs = 0;
        this.numPasses = 0;
    }

    public void accumulate(RAValue extract) {
        numPasses++;
        numObs += extract.getNumPixel();
        float[][] samples = extract.getSamples();
        for (int bandId = 0; bandId < samples.length; bandId++) {
            accus[bandId].accumulate(samples[bandId]);
        }
    }

    private void finish() {
        bandStats = new StatiticsComputer[accus.length];
        for (int i = 0; i < accus.length; i++) {
            bandStats[i] = new StatiticsComputer(accus[i].getBandname(), accus[i].getValues());
        }
    }

    private List<String> getHeader() {
        List<String> header = new ArrayList<>();
        header.add("RegionId");
        header.add("TimeWindow_start");
        header.add("TimeWindow_end");
        header.add("numPasses");
        header.add("numObs");
        for (StatiticsComputer bandStat : bandStats) {
            header.addAll(bandStat.getHeader());
        }
        return header;
    }

    private List<String> getStats() {
        List<String> stats = new ArrayList<>();
        stats.add(regionName);
        stats.add(dStart);
        stats.add(dEnd);
        stats.add(Integer.toString(numPasses));
        stats.add(Integer.toString(numObs));
        for (StatiticsComputer bandStat : bandStats) {
            stats.addAll(bandStat.getStats());
        }
        return stats;
    }

    public void write(Writer writer) throws IOException {
        if (!headerWritten) {
            writeLine(writer, getHeader());
            headerWritten = true;
        }
        finish();
        writeLine(writer, getStats());
    }

    private static void writeLine(Writer writer, List<String> fields) throws IOException {
        writer.write(String.join("\t", fields) + "\n");
    }
}
