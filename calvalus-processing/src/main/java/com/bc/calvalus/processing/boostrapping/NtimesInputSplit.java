package com.bc.calvalus.processing.boostrapping;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class NtimesInputSplit extends InputSplit implements Writable {

    private static final String[] EMPTY_HOSTS = new String[0];

    private int numberOfIterations;
    private String[] hosts = EMPTY_HOSTS;

    // for deserialization
    public NtimesInputSplit() {
        this(0);
    }

    public NtimesInputSplit(int numberOfIterations) {
        this.numberOfIterations = numberOfIterations;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return numberOfIterations;
    }

    @Override
    public String[] getLocations() throws IOException {
        return this.hosts;
    }

    ////////////////////////////////////////////
    // Writable methods
    ////////////////////////////////////////////

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(numberOfIterations);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        numberOfIterations = in.readInt();
        hosts = new String[]{};
    }
}
