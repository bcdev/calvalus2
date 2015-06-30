package com.bc.calvalus.processing.ta;

import com.bc.calvalus.processing.l3.L3TemporalBin;
import org.apache.hadoop.io.Writable;
import org.esa.snap.binning.TemporalBin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * A Hadoop-serializable, temporal bin that in addition writes out its bin index and the time.
 * Required for TA.
 * The class is final for allowing method in-lining.
 *
 * @author Martin
 */
public final class L3TemporalBinWithIndex extends TemporalBin implements Writable {

    long time;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public L3TemporalBinWithIndex() {
        super();
    }

    public L3TemporalBinWithIndex(L3TemporalBin l3Bin, long time) {
        super(l3Bin.getIndex(), l3Bin.getFeatureValues().length);
        this.time = time;
        setNumObs(l3Bin.getNumObs());
        setNumPasses(l3Bin.getNumPasses());
        for (int i=0; i<l3Bin.getFeatureValues().length; ++i) {
            getFeatureValues()[i] = l3Bin.getFeatureValues()[i];
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(time);
        dataOutput.writeLong(getIndex());
        super.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        time = dataInput.readLong();
        setIndex(dataInput.readLong());
        super.readFields(dataInput);
    }

    public static L3TemporalBinWithIndex read(DataInput dataInput) throws IOException {
        L3TemporalBinWithIndex bin = new L3TemporalBinWithIndex();
        bin.readFields(dataInput);
        return bin;
    }
}
