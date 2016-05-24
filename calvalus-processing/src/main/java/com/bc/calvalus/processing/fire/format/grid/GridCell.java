package com.bc.calvalus.processing.fire.format.grid;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author thomas
 */
public class GridCell implements Writable {

    private static final int BAND_SIZE = GridMapper.TARGET_RASTER_WIDTH * GridMapper.TARGET_RASTER_HEIGHT;

    int[] baFirstHalf;
    int[] baSecondHalf;
    float[] patchNumberFirstHalf;
    float[] patchNumberSecondHalf;
    int[] errorsFirstHalf;
    int[] errorsSecondHalf;
    List<int[]> baInLcFirstHalf;
    List<int[]> baInLcSecondHalf;
    float[] coverageFirstHalf;
    float[] coverageSecondHalf;

    void setBaFirstHalf(int[] baFirstHalf) {
        this.baFirstHalf = baFirstHalf;
    }

    void setBaSecondHalf(int[] baSecondHalf) {
        this.baSecondHalf = baSecondHalf;
    }

    void setPatchNumberFirstHalf(float[] patchNumberFirstHalf) {
        this.patchNumberFirstHalf = patchNumberFirstHalf;
    }

    void setPatchNumberSecondHalf(float[] patchNumberSecondHalf) {
        this.patchNumberSecondHalf = patchNumberSecondHalf;
    }

    void setErrorsFirstHalf(int[] errorsFirstHalf) {
        this.errorsFirstHalf = errorsFirstHalf;
    }

    void setErrorsSecondHalf(int[] errorsSecondHalf) {
        this.errorsSecondHalf = errorsSecondHalf;
    }

    void setBaInLcFirstHalf(List<int[]> baInLcFirstHalf) {
        this.baInLcFirstHalf = baInLcFirstHalf;
    }

    void setBaInLcSecondHalf(List<int[]> baInLcSecondHalf) {
        this.baInLcSecondHalf = baInLcSecondHalf;
    }

    void setCoverageFirstHalf(float[] coverageFirstHalf) {
        this.coverageFirstHalf = coverageFirstHalf;
    }

    void setCoverageSecondHalf(float[] coverageSecondHalf) {
        this.coverageSecondHalf = coverageSecondHalf;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (int v : baFirstHalf) {
            out.writeInt(v);
        }
        for (int v : baSecondHalf) {
            out.writeInt(v);
        }
        for (float v : patchNumberFirstHalf) {
            out.writeFloat(v);
        }
        for (float v : patchNumberSecondHalf) {
            out.writeFloat(v);
        }
        for (int v : errorsFirstHalf) {
            out.writeInt(v);
        }
        for (int v : errorsSecondHalf) {
            out.writeInt(v);
        }
        for (int[] lcClass : baInLcFirstHalf) {
            for (int value : lcClass) {
                out.writeInt(value);
            }
        }
        for (int[] lcClass : baInLcSecondHalf) {
            for (int value : lcClass) {
                out.writeInt(value);
            }
        }
        for (float v : coverageFirstHalf) {
            out.writeFloat(v);
        }
        for (float v : coverageSecondHalf) {
            out.writeFloat(v);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        baFirstHalf = new int[BAND_SIZE];
        baSecondHalf = new int[BAND_SIZE];
        patchNumberFirstHalf = new float[BAND_SIZE];
        patchNumberSecondHalf = new float[BAND_SIZE];
        errorsFirstHalf = new int[BAND_SIZE];
        errorsSecondHalf = new int[BAND_SIZE];
        baInLcFirstHalf = new ArrayList<>();
        baInLcSecondHalf = new ArrayList<>();
        for (int lcClass = 0; lcClass < GridMapper.LC_CLASSES_COUNT; lcClass++) {
            baInLcFirstHalf.add(new int[BAND_SIZE]);
            baInLcSecondHalf.add(new int[BAND_SIZE]);
        }
        coverageFirstHalf = new float[BAND_SIZE];
        coverageSecondHalf = new float[BAND_SIZE];

        for (int i = 0; i < BAND_SIZE; i++) {
            baFirstHalf[i] = in.readInt();
        }
        for (int i = 0; i < BAND_SIZE; i++) {
            baSecondHalf[i] = in.readInt();
        }
        for (int i = 0; i < BAND_SIZE; i++) {
            patchNumberFirstHalf[i] = in.readFloat();
        }
        for (int i = 0; i < BAND_SIZE; i++) {
            patchNumberSecondHalf[i] = in.readFloat();
        }
        for (int i = 0; i < BAND_SIZE; i++) {
            errorsFirstHalf[i] = in.readInt();
        }
        for (int i = 0; i < BAND_SIZE; i++) {
            errorsSecondHalf[i] = in.readInt();
        }
        for (int[] lcClass : baInLcFirstHalf) {
            for (int i = 0; i < lcClass.length; i++) {
                lcClass[i] = in.readInt();
            }
        }
        for (int[] lcClass : baInLcSecondHalf) {
            for (int i = 0; i < lcClass.length; i++) {
                lcClass[i] = in.readInt();
            }
        }
        for (int i = 0; i < BAND_SIZE; i++) {
            coverageFirstHalf[i] = in.readFloat();
        }
        for (int i = 0; i < BAND_SIZE; i++) {
            coverageSecondHalf[i] = in.readFloat();
        }
    }
}
