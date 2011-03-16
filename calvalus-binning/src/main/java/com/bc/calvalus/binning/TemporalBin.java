package com.bc.calvalus.binning;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;


/**
 * A Hadoop-serializable, temporal bin.
 * The class is final for allowing method in-lining.
 *
 * @author Norman Fomferra
 */
public final class TemporalBin extends Bin {

    int numPasses;

    public TemporalBin() {
        super();
    }

    public TemporalBin(long index, int numProperties) {
        super(index, numProperties);
    }

    public int getNumPasses() {
        return numPasses;
    }

    public void setNumPasses(int numPasses) {
        this.numPasses = numPasses;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // Note, we don't serialise the index, because it is usually the MapReduce key
        dataOutput.writeInt(numObs);
        dataOutput.writeInt(numPasses);
        dataOutput.writeInt(properties.length);
        for (float property : properties) {
            dataOutput.writeFloat(property);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // Note, we don't serialise the index, because it is usually the MapReduce key
        numObs = dataInput.readInt();
        numPasses = dataInput.readInt();
        final int numProps = dataInput.readInt();
        properties = new float[numProps];
        for (int i = 0; i < numProps; i++) {
            properties[i] = dataInput.readFloat();
        }
    }

    public static TemporalBin read(DataInput dataInput) throws IOException {
        TemporalBin bin = new TemporalBin();
        bin.readFields(dataInput);
        return bin;
    }

    @Override
    public String toString() {
        return String.format("%s{index=%d, numObs=%d, numPasses=%d, properties=%s}",
                             getClass().getSimpleName(), index, numObs, numPasses, Arrays.toString(properties));
    }

}
