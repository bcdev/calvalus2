package com.bc.calvalus.processing.l3.seasonal;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class BandTileWritable implements Writable {

    public BandTileWritable() {}

    float[] tileData;

    public float[] getTileData() {
        return tileData;
    }
    public BandTileWritable(float[] tileData) {
        this.tileData = tileData;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(tileData.length);
        for (float f : tileData) {
            out.writeFloat(f);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        final int length = in.readInt();
        if (tileData == null) {
            tileData = new float[length];
        }
        for (int i = 0; i < length; ++i) {
            tileData[i] = in.readFloat();
        }
    }
}
