package com.bc.calvados.hadoop.n1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class N1LineInputSplit extends InputSplit implements Writable {

    private Path file;
    private long start;
    private long length;
    private String[] hosts;

    private int yStart;
    private int height;

    public N1LineInputSplit() {
    }

    public N1LineInputSplit(Path file, long start, long length, String[] hosts, int yStart, int height) {
        this.file = file;
        this.start = start;
        this.length = length;
        this.hosts = hosts;
        this.yStart = yStart;
        this.height = height;
    }

    public int getYStart() {
        return yStart;
    }

    public int getHeight() {
        return height;
    }

    /** The file containing this split's data. */
    public Path getPath() { return file; }

    /** The position of the first byte in the file to process. */
    public long getStart() { return start; }

    /** The number of bytes in the file to process. */
    @Override
    public long getLength() { return length; }

    @Override
    public String toString() { return file + ":" + start + "+" + length; }

    ////////////////////////////////////////////
    // Writable methods
    ////////////////////////////////////////////

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, file.toString());
        out.writeLong(start);
        out.writeLong(length);
        out.writeInt(yStart);
        out.writeInt(height);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        file = new Path(Text.readString(in));
        start = in.readLong();
        length = in.readLong();
        yStart = in.readInt();
        height = in.readInt();
        hosts = null;
    }

    @Override
    public String[] getLocations() throws IOException {
        if (this.hosts == null) {
            return new String[]{};
        } else {
            return this.hosts;
        }
    }

}
