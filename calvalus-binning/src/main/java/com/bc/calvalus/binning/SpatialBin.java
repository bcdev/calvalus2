package com.bc.calvalus.binning;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;


/**
 * A Hadoop-serializable, spatial bin.
 * The class is final for allowing method in-lining.
 *
 * @author Norman Fomferra
 */
public final class SpatialBin extends Bin implements Writable {

    public SpatialBin() {
        super();
    }

    public SpatialBin(long index, int numFeatures) {
        super(index, numFeatures);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // Note, we don't serialise the index, because it is usually the MapReduce key
        dataOutput.writeInt(numObs);
        dataOutput.writeInt(featureValues.length);
        for (float value : featureValues) {
            dataOutput.writeFloat(value);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // // Note, we don't serialise the index, because it is usually the MapReduce key
        numObs = dataInput.readInt();
        final int numFeatures = dataInput.readInt();
        featureValues = new float[numFeatures];
        for (int i = 0; i < numFeatures; i++) {
            featureValues[i] = dataInput.readFloat();
        }
    }

    public static SpatialBin read(DataInput dataInput) throws IOException {
        SpatialBin bin = new SpatialBin();
        bin.readFields(dataInput);
        return bin;
    }

    @Override
    public String toString() {
        return String.format("%s{index=%d, numObs=%d, featureValues=%s}",
                             getClass().getSimpleName(), index, numObs, Arrays.toString(featureValues));
    }
}
