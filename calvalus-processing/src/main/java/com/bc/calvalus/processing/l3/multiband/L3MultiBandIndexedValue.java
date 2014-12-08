package com.bc.calvalus.processing.l3.multiband;

import org.apache.hadoop.io.Writable;
import org.esa.beam.binning.TemporalBin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;


/**
 * A Hadoop-serializable, bin for multi band formatting.
 * It also serializes the bin index
 *
 * The class is final for allowing method in-lining.
 *
 * @author Boe
 */
public final class L3MultiBandIndexedValue implements Writable {

    long index;
    float value;

    public L3MultiBandIndexedValue() {
        super();
    }

    public L3MultiBandIndexedValue(long index, float value) {
        this.index = index;
        this.value = value;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(getIndex());
        dataOutput.writeFloat(getValue());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        setIndex(dataInput.readLong());
        setValue(dataInput.readFloat());
    }

    @Override
    public String toString() {
        return String.format("%s{index=%d, numObs=%d, numPasses=%d, featureValues=%s}",
                                     getClass().getSimpleName(),
                                     getIndex(),
                                     getValue());
    }
}
