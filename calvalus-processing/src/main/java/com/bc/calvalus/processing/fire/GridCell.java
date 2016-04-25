package com.bc.calvalus.processing.fire;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author thomas
 */
public class GridCell implements Writable {

    private static final int NUM_LC_CLASSES = 18;
    private static final int BAND_SIZE = 40 * 40;

    float[][] data;

    public GridCell() {
        this.data = new float[8 + 2 * NUM_LC_CLASSES][BAND_SIZE]; // BA, error, coverage, patches, lc; each for 07 and 22 of month
    }

    public void setData(int bandId, float[] data) {
        this.data[bandId] = data;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (float[] band : data) {
            if (band != null) {
                // todo - can remove null check once it is done
                for (float v : band) {
                    out.writeFloat(v);
                }
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for (int bandIdx = 0; bandIdx < 2; bandIdx++) {
            float[] band = new float[BAND_SIZE];
            data[bandIdx] = band;
            System.out.println("Reading some data:");
            for (int i = 0; i < BAND_SIZE; i++) {
                band[i] = in.readFloat();
            }
        }
    }
}
