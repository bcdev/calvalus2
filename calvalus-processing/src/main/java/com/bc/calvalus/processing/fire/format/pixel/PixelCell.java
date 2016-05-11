package com.bc.calvalus.processing.fire.format.pixel;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author thomas
 */
public class PixelCell implements Writable {

    private static final int BAND_SIZE = PixelMapper.RASTER_WIDTH * PixelMapper.RASTER_HEIGHT;

    int[] values;

    @Override
    public void write(DataOutput out) throws IOException {
        for (int v : values) {
            out.writeInt(v);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        values = new int[BAND_SIZE];
        for (int i = 0; i < BAND_SIZE; i++) {
            values[i] = in.readInt();
        }
    }
}
