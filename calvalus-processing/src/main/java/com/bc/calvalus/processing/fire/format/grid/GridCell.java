package com.bc.calvalus.processing.fire.format.grid;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.LC_CLASSES_COUNT;

/**
 * @author thomas
 */
public class GridCell implements Writable {

    public float[] baFirstHalf;
    public float[] baSecondHalf;
    public float[] patchNumberFirstHalf;
    public float[] patchNumberSecondHalf;
    public float[] errorsFirstHalf;
    public float[] errorsSecondHalf;
    public List<float[]> baInLcFirstHalf;
    public List<float[]> baInLcSecondHalf;
    public float[] coverageFirstHalf;
    public float[] coverageSecondHalf;
    public float[] burnableFraction;
    public int bandSize;

    void setBaFirstHalf(float[] baFirstHalf) {
        this.baFirstHalf = baFirstHalf;
    }

    void setBaSecondHalf(float[] baSecondHalf) {
        this.baSecondHalf = baSecondHalf;
    }

    void setPatchNumberFirstHalf(float[] patchNumberFirstHalf) {
        this.patchNumberFirstHalf = patchNumberFirstHalf;
    }

    void setPatchNumberSecondHalf(float[] patchNumberSecondHalf) {
        this.patchNumberSecondHalf = patchNumberSecondHalf;
    }

    void setErrorsFirstHalf(float[] errorsFirstHalf) {
        this.errorsFirstHalf = errorsFirstHalf;
    }

    void setErrorsSecondHalf(float[] errorsSecondHalf) {
        this.errorsSecondHalf = errorsSecondHalf;
    }

    void setBaInLcFirstHalf(List<float[]> baInLcFirstHalf) {
        this.baInLcFirstHalf = baInLcFirstHalf;
    }

    void setBaInLcSecondHalf(List<float[]> baInLcSecondHalf) {
        this.baInLcSecondHalf = baInLcSecondHalf;
    }

    void setCoverageFirstHalf(float[] coverageFirstHalf) {
        this.coverageFirstHalf = coverageFirstHalf;
    }

    void setCoverageSecondHalf(float[] coverageSecondHalf) {
        this.coverageSecondHalf = coverageSecondHalf;
    }

    void setBurnableFraction(float[] burnableFraction) {
        this.burnableFraction = burnableFraction;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(bandSize);
        for (float v : baFirstHalf) {
            out.writeFloat((int) v);
        }
        for (float v : baSecondHalf) {
            out.writeFloat((int) v);
        }
        for (float v : patchNumberFirstHalf) {
            out.writeFloat(v);
        }
        for (float v : patchNumberSecondHalf) {
            out.writeFloat(v);
        }
        for (float v : errorsFirstHalf) {
            out.writeFloat((int) v);
        }
        for (float v : errorsSecondHalf) {
            out.writeFloat((int) v);
        }
        for (float[] lcClass : baInLcFirstHalf) {
            for (float value : lcClass) {
                out.writeFloat((int) value);
            }
        }
        for (float[] lcClass : baInLcSecondHalf) {
            for (float value : lcClass) {
                out.writeFloat((int) value);
            }
        }
        for (float v : coverageFirstHalf) {
            out.writeFloat(v);
        }
        for (float v : coverageSecondHalf) {
            out.writeFloat(v);
        }
        for (float v : burnableFraction) {
            out.writeFloat(v);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int bandSize = in.readInt();
        baFirstHalf = new float[bandSize];
        baSecondHalf = new float[bandSize];
        patchNumberFirstHalf = new float[bandSize];
        patchNumberSecondHalf = new float[bandSize];
        errorsFirstHalf = new float[bandSize];
        errorsSecondHalf = new float[bandSize];
        baInLcFirstHalf = new ArrayList<>();
        baInLcSecondHalf = new ArrayList<>();
        burnableFraction = new float[bandSize];
        for (int lcClass = 0; lcClass < LC_CLASSES_COUNT; lcClass++) {
            baInLcFirstHalf.add(new float[bandSize]);
            baInLcSecondHalf.add(new float[bandSize]);
        }
        coverageFirstHalf = new float[bandSize];
        coverageSecondHalf = new float[bandSize];

        for (int i = 0; i < bandSize; i++) {
            baFirstHalf[i] = in.readFloat();
        }
        for (int i = 0; i < bandSize; i++) {
            baSecondHalf[i] = in.readFloat();
        }
        for (int i = 0; i < bandSize; i++) {
            patchNumberFirstHalf[i] = in.readFloat();
        }
        for (int i = 0; i < bandSize; i++) {
            patchNumberSecondHalf[i] = in.readFloat();
        }
        for (int i = 0; i < bandSize; i++) {
            errorsFirstHalf[i] = in.readFloat();
        }
        for (int i = 0; i < bandSize; i++) {
            errorsSecondHalf[i] = in.readFloat();
        }
        for (float[] lcClass : baInLcFirstHalf) {
            for (int i = 0; i < lcClass.length; i++) {
                lcClass[i] = in.readFloat();
            }
        }
        for (float[] lcClass : baInLcSecondHalf) {
            for (int i = 0; i < lcClass.length; i++) {
                lcClass[i] = in.readFloat();
            }
        }
        for (int i = 0; i < bandSize; i++) {
            coverageFirstHalf[i] = in.readFloat();
        }
        for (int i = 0; i < bandSize; i++) {
            coverageSecondHalf[i] = in.readFloat();
        }
        for (int i = 0; i < bandSize; i++) {
            burnableFraction[i] = in.readFloat();
        }
    }

    @Override
    public String toString() {
        return "GridCell{" +
                "baFirstHalf=" + Arrays.toString(baFirstHalf) +
                ", baSecondHalf=" + Arrays.toString(baSecondHalf) +
                ", patchNumberFirstHalf=" + Arrays.toString(patchNumberFirstHalf) +
                ", patchNumberSecondHalf=" + Arrays.toString(patchNumberSecondHalf) +
                ", errorsFirstHalf=" + Arrays.toString(errorsFirstHalf) +
                ", errorsSecondHalf=" + Arrays.toString(errorsSecondHalf) +
                ", baInLcFirstHalf=" + baInLcFirstHalf +
                ", baInLcSecondHalf=" + baInLcSecondHalf +
                ", coverageFirstHalf=" + Arrays.toString(coverageFirstHalf) +
                ", coverageSecondHalf=" + Arrays.toString(coverageSecondHalf) +
                ", burnable=" + Arrays.toString(burnableFraction) +
                '}';
    }
}
