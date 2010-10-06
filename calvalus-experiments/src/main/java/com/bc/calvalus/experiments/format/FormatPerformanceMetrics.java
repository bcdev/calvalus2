package com.bc.calvalus.experiments.format;

/**
 * Stores read and write times in nanoseconds.
 */
public class FormatPerformanceMetrics {
    public final long numBytesRead;
    public final long readTime;

    public final long numBytesWritten;
    public final long writeTime;

    public FormatPerformanceMetrics(long numBytesRead, long readTime, 
                                    long numBytesWritten, long writeTime) {
        this.numBytesRead = numBytesRead;
        this.readTime = readTime;
        this.numBytesWritten = numBytesWritten;
        this.writeTime = writeTime;
    }
}
