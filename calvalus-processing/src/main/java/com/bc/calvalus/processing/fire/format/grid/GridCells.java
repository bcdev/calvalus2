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
 * Represents a set of target grid cells; each grid cell value is written into the final grid product.
 *
 * @author thomas
 */
public class GridCells implements Writable {

    public float[] ba;
    public float[] patchNumber;
    public float[] errors;
    public List<float[]> baInLc;
    public float[] coverage;
    public float[] burnableFraction;
    public int bandSize;

    void setBa(float[] ba) {
        this.ba = ba;
    }

    void setPatchNumber(float[] patchNumber) {
        this.patchNumber = patchNumber;
    }

    void setErrors(float[] errors) {
        this.errors = errors;
    }

    void setBaInLc(List<float[]> baInLc) {
        this.baInLc = baInLc;
    }

    void setCoverage(float[] coverage) {
        this.coverage = coverage;
    }

    void setBurnableFraction(float[] burnableFraction) {
        this.burnableFraction = burnableFraction;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(bandSize);
        for (float v : ba) {
            out.writeFloat((int) v);
        }
        for (float v : patchNumber) {
            out.writeFloat(v);
        }
        for (float v : errors) {
            out.writeFloat((int) v);
        }
        for (float[] lcClass : baInLc) {
            for (float value : lcClass) {
                out.writeFloat((int) value);
            }
        }
        for (float v : coverage) {
            out.writeFloat(v);
        }
        for (float v : burnableFraction) {
            out.writeFloat(v);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int bandSize = in.readInt();
        ba = new float[bandSize];
        patchNumber = new float[bandSize];
        errors = new float[bandSize];
        baInLc = new ArrayList<>();
        burnableFraction = new float[bandSize];
        for (int lcClass = 0; lcClass < LC_CLASSES_COUNT; lcClass++) {
            baInLc.add(new float[bandSize]);
        }
        coverage = new float[bandSize];

        for (int i = 0; i < bandSize; i++) {
            ba[i] = in.readFloat();
        }
        for (int i = 0; i < bandSize; i++) {
            patchNumber[i] = in.readFloat();
        }
        for (int i = 0; i < bandSize; i++) {
            errors[i] = in.readFloat();
        }
        for (float[] lcClass : baInLc) {
            for (int i = 0; i < lcClass.length; i++) {
                lcClass[i] = in.readFloat();
            }
        }
        for (int i = 0; i < bandSize; i++) {
            coverage[i] = in.readFloat();
        }
        for (int i = 0; i < bandSize; i++) {
            burnableFraction[i] = in.readFloat();
        }
    }

    @Override
    public String toString() {
        return "GridCells{" +
                "ba=" + Arrays.toString(ba) +
                ", patchNumber=" + Arrays.toString(patchNumber) +
                ", errors=" + Arrays.toString(errors) +
                ", baInLc=" + baInLc +
                ", coverage=" + Arrays.toString(coverage) +
                ", burnable=" + Arrays.toString(burnableFraction) +
                '}';
    }
}
