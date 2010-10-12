package com.bc.calvalus.experiments.processing;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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

    private int yStart;
    private int height;

    public N1InterleavedInputSplit() {
        super(null, 0, 0, null);
    }

    public N1InterleavedInputSplit(Path file,
                                   long start,
                                   long length,
                                   String[] hosts,
                                   int yStart,
                                   int height) {
        super(file, start, length, hosts);
        this.yStart = yStart;
        this.height = height;
    }

    /** @return start record number of split */
    public int getYStart() {
        return yStart;
    }

    /** @return number of records of split */
    public int getHeight() {
        return height;
    }

    @Override
    public String toString() {
        return getPath() + ":" + getStart() + "+" + getLength()
               + " (" + getYStart() + "+" + getHeight() + ")";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(yStart);
        out.writeInt(height);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        yStart = in.readInt();
        height = in.readInt();
    }

    /**
     * Static factory, unfortunately has to expand code from FileSplit as this does not have a public default constructor
     *
     * @param in  DataInput to read the values of one split from
     * @return  Newly constructed N1InterleavedInputSplit
     * @throws IOException  if values cannot be read
     */
//    public static N1InterleavedInputSplit read(DataInput in) throws IOException {
//        Path file = new Path(Text.readString(in));
//        long start = in.readLong();
//        long length = in.readLong();
//        String[] hosts = null;
//        int yStart = in.readInt();
//        int height = in.readInt();
//        return new N1InterleavedInputSplit(file, start, length, hosts, yStart, height);
//    }
}
