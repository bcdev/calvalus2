package com.bc.calvalus.processing.fire;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author thomas
 */
public class PixelCell implements Writable {

    private static final int BAND_SIZE = FirePixelMapper.RASTER_WIDTH * FirePixelMapper.RASTER_HEIGHT;

    int[] doy;
    int[] error;
    int[] lcClass;

    @Override
    public void write(DataOutput out) throws IOException {
        for (int v : doy) {
            out.writeInt(v);
        }
        for (int v : error) {
            out.writeInt(v);
        }
        for (int v : lcClass) {
            out.writeInt(v);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        doy = new int[BAND_SIZE];
        error = new int[BAND_SIZE];
        lcClass = new int[BAND_SIZE];

        for (int i = 0; i < BAND_SIZE; i++) {
            doy[i] = in.readInt();
        }
        for (int i = 0; i < BAND_SIZE; i++) {
            error[i] = in.readInt();
        }
        for (int i = 0; i < BAND_SIZE; i++) {
            lcClass[i] = in.readInt();
        }
    }
}
