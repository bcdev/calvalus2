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

    public static final int TILE_HEIGHT = 1800;
    public static final int TILE_WIDTH = 1800;

    float[] tileData;

    public float[] getTileData() {
        return tileData;
    }
    public BandTileWritable(float[] tileData) {
        this.tileData = tileData;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (float f : tileData) {
            out.writeFloat(f);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (tileData == null) {
            tileData = new float[TILE_HEIGHT * TILE_WIDTH];
        }
        for (int i = 0; i < TILE_HEIGHT * TILE_WIDTH; ++i) {
            tileData[i] = in.readFloat();
        }
    }
}
