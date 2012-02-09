package com.bc.calvalus.processing.l3;

import com.bc.calvalus.binning.TemporalBin;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * A Hadoop-serializable, temporal bin.
 * The class is final for allowing method in-lining.
 *
 * @author Norman Fomferra
 */
public final class L3TemporalBin extends TemporalBin implements Writable {

    public L3TemporalBin() {
        super();
    }

    public L3TemporalBin(long index, int numFeatures) {
        super(index, numFeatures);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
    }

    public static L3TemporalBin read(DataInput dataInput) throws IOException {
        L3TemporalBin bin = new L3TemporalBin();
        bin.readFields(dataInput);
        return bin;
    }
}
