package com.bc.calvalus.b3;

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
public final class SpatialBin implements Writable {

    int index;
    int numObs;
    float[] properties;

    public SpatialBin() {
        this.index = -1;
    }

    public SpatialBin(int index, int numProperties) {
        if (numProperties < 0) {
            throw new IllegalArgumentException("numProperties < 0");
        }
        this.index = index;
        this.properties = new float[numProperties];
    }

    public int getIndex() {
        return index;
    }

    public int getNumObs() {
        return numObs;
    }

    public int getPropertyCount() {
        return properties.length;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // Note, we don't serialise the index, because it is usually the MapReduce key
        dataOutput.writeInt(numObs);
        dataOutput.writeInt(properties.length);
        for (float property : properties) {
            dataOutput.writeFloat(property);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // // Note, we don't serialise the index, because it is usually the MapReduce key
        numObs = dataInput.readInt();
        final int numProps = dataInput.readInt();
        properties = new float[numProps];
        for (int i = 0; i < numProps; i++) {
            properties[i] = dataInput.readFloat();
        }
    }

    public static SpatialBin read(DataInput dataInput) throws IOException {
        SpatialBin bin = new SpatialBin();
        bin.readFields(dataInput);
        return bin;
    }

    @Override
    public String toString() {
        return String.format("%s{index=%d, numObs=%d, properties=%s}",
                             getClass().getSimpleName(), index, numObs, Arrays.toString(properties));
    }
}
