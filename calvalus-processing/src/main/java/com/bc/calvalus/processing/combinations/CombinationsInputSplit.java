package com.bc.calvalus.processing.combinations;

import com.bc.ceres.core.Assert;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class CombinationsInputSplit extends InputSplit implements Writable {

    private static final String[] EMPTY_ARRAY = new String[0];

    private String[] values;
    private String[] hosts = EMPTY_ARRAY;

    // for deserialization
    public CombinationsInputSplit() {
        this(EMPTY_ARRAY);
    }

    public CombinationsInputSplit(String[] values) {
        Assert.notNull(values);
        this.values = values;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return values.length;
    }

    @Override
    public String[] getLocations() throws IOException {
        return this.hosts;
    }

    public String[] getValues() {
        return values;
    }

    ////////////////////////////////////////////
    // Writable methods
    ////////////////////////////////////////////

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.length);
        for (String value : values) {
            out.writeUTF(value);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int arrayLength = in.readInt();
        values = new String[arrayLength];
        for (int i = 0; i < values.length; i++) {
            values[i] = in.readUTF();
        }
        hosts = EMPTY_ARRAY;
    }
}
