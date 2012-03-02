package com.bc.calvalus.processing.l3;

import org.esa.beam.binning.SpatialBin;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * A Hadoop-serializable, spatial bin.
 * The class is final for allowing method in-lining.
 *
 * @author Norman Fomferra
 */
public final class L3SpatialBin extends SpatialBin implements Writable {

    public L3SpatialBin() {
        super();
    }

    public L3SpatialBin(long index, int numFeatures) {
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

    public static L3SpatialBin read(DataInput dataInput) throws IOException {
        L3SpatialBin bin = new L3SpatialBin();
        bin.readFields(dataInput);
        return bin;
    }
}
