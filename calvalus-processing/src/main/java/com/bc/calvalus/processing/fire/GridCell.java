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
    float[] errorsFirstHalf;
    float[] errorsSecondHalf;

    void setBaFirstHalf(float[] baFirstHalf) {
        this.baFirstHalf = baFirstHalf;
    }

    void setBaSecondHalf(float[] baSecondHalf) {
        this.baSecondHalf = baSecondHalf;
    }

    void setPatchNumberFirstHalf(int[] patchNumberFirstHalf) {
        this.patchNumberFirstHalf = patchNumberFirstHalf;
    }

    void setPatchNumberSecondHalf(int[] patchNumberSecondHalf) {
        this.patchNumberSecondHalf = patchNumberSecondHalf;
    }

    void setErrorsFirstHalf(float[] errorsFirstHalf) {
        this.errorsFirstHalf = errorsFirstHalf;
    }

    void setErrorsSecondHalf(float[] errorsSecondHalf) {
        this.errorsSecondHalf = errorsSecondHalf;
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
        for (float v : errorsFirstHalf) {
            out.writeFloat(v);
        }
        for (float v : errorsSecondHalf) {
            out.writeFloat(v);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        baFirstHalf = new float[BAND_SIZE];
        baSecondHalf = new float[BAND_SIZE];
        patchNumberFirstHalf = new int[BAND_SIZE];
        patchNumberSecondHalf = new int[BAND_SIZE];
        errorsFirstHalf = new float[BAND_SIZE];
        errorsSecondHalf = new float[BAND_SIZE];

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
        for (int i = 0; i < BAND_SIZE; i++) {
            errorsFirstHalf[i] = in.readFloat();
        }
        for (int i = 0; i < BAND_SIZE; i++) {
            errorsSecondHalf[i] = in.readFloat();
        }
    }
}
