package com.bc.calvalus.processing.fire.format.pixel.meris;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author thomas
 */
public class MerisPixelCell implements Writable {

    private static final int BAND_SIZE = MerisPixelMapper.RASTER_WIDTH * MerisPixelMapper.RASTER_HEIGHT;

    short[] values;

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
