package com.bc.calvalus.experiments.format;

/**
 * Stores read and write times in nanoseconds.
 */
public class FormatPerformanceMetrics {
    public long numBytesRead;
    public long readTime;

    public long numBytesWritten;
    public long writeTime;

    public FormatPerformanceMetrics(long numBytesRead, long readTime, 
                                    long numBytesWritten, long writeTime) {
        this.numBytesRead = numBytesRead;
        this.readTime = readTime;
        this.numBytesWritten = numBytesWritten;
        this.writeTime = writeTime;
    }

    public void add(FormatPerformanceMetrics measurement) {
        numBytesRead += measurement.numBytesRead;
        readTime += measurement.readTime;
        numBytesWritten += measurement.numBytesWritten;
        writeTime += measurement.writeTime;
    }

    public String toString() {
        return numBytesRead + " bytes read in " + (readTime / 10e9) + " sec, " + numBytesWritten + " bytes written in " + (writeTime / 10e9) + " sec";
    }
}
