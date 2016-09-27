package com.bc.calvalus.processing.fire.format.pixel;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author thomas
 */
public class PixelCell implements Writable {

    private static int BAND_SIZE = 0;

    short[] values;

    public PixelCell(int rasterWidth, int rasterHeight) {
        BAND_SIZE = rasterWidth * rasterHeight;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (short v : values) {
            out.writeShort(v);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        values = new short[BAND_SIZE];
        for (int i = 0; i < BAND_SIZE; i++) {
            values[i] = in.readShort();
        }
    }
}
