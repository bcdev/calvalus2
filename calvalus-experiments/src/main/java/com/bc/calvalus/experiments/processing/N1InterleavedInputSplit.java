package com.bc.calvalus.experiments.processing;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Split data structure for line-interleaved N1 format, providing
 * start record number and number of records of split
 * in addition to byte start and number of bytes.
 *
 * @author Martin Boettcher
 */
public class N1InterleavedInputSplit extends FileSplit implements Writable {

    private int startRecord;
    private int numberOfRecords;

    /**
     * called by Hadoop using reflection
     */
    public N1InterleavedInputSplit() {
        super(null, 0, 0, null);
    }

    public N1InterleavedInputSplit(Path file,
                                   long start,
                                   long length,
                                   String[] hosts,
                                   int yStart,
                                   int numberOfRecords) {
        super(file, start, length, hosts);
        this.startRecord = yStart;
        this.numberOfRecords = numberOfRecords;
    }

    /** @return start record number of split */
    public int getStartRecord() {
        return startRecord;
    }

    /** @return number of records of split */
    public int getNumberOfRecords() {
        return numberOfRecords;
    }

    @Override
    public String toString() {
        return getPath() + ":" + getStart() + "+" + getLength()
               + " (" + getStartRecord() + "+" + getNumberOfRecords() + ")";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(startRecord);
        out.writeInt(numberOfRecords);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        startRecord = in.readInt();
        numberOfRecords = in.readInt();
    }
}
