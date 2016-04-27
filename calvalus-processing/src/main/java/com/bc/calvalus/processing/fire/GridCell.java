package com.bc.calvalus.processing.fire;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author thomas
 */
public class GridCell implements Writable {

    private static final int BAND_SIZE = 40 * 40;

    float[] baFirstHalf;
    float[] baSecondHalf;
    int[] patchNumberFirstHalf;
    int[] patchNumberSecondHalf;

    public void setBaFirstHalf(float[] baFirstHalf) {
        this.baFirstHalf = baFirstHalf;
    }

    public void setBaSecondHalf(float[] baSecondHalf) {
        this.baSecondHalf = baSecondHalf;
    }

    public void setPatchNumberFirstHalf(int[] patchNumberFirstHalf) {
        this.patchNumberFirstHalf = patchNumberFirstHalf;
    }

    public void setPatchNumberSecondHalf(int[] patchNumberSecondHalf) {
        this.patchNumberSecondHalf = patchNumberSecondHalf;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (float v : baFirstHalf) {
            out.writeFloat(v);
        }
        for (float v : baSecondHalf) {
            out.writeFloat(v);
        }
        for (int v : patchNumberFirstHalf) {
            out.writeInt(v);
        }
        for (int v : patchNumberSecondHalf) {
            out.writeInt(v);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        baFirstHalf = new float[BAND_SIZE];
        baSecondHalf = new float[BAND_SIZE];
        patchNumberFirstHalf = new int[BAND_SIZE];
        patchNumberSecondHalf = new int[BAND_SIZE];

        for (int i = 0; i < BAND_SIZE; i++) {
            baFirstHalf[i] = in.readFloat();
        }
        for (int i = 0; i < BAND_SIZE; i++) {
            baSecondHalf[i] = in.readFloat();
        }
        for (int i = 0; i < BAND_SIZE; i++) {
            patchNumberFirstHalf[i] = in.readInt();
        }
        for (int i = 0; i < BAND_SIZE; i++) {
            patchNumberSecondHalf[i] = in.readInt();
        }
    }
}
