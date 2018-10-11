package com.bc.calvalus.processing.fire.format.grid;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a set of target grid cells; each grid cell value is written into the final grid product.
 *
 * @author thomas
 */
public class GridCells implements Writable {

    public int lcClassesCount;
    public double[] ba;
    public float[] patchNumber;
    public float[] errors;
    public List<double[]> baInLc;
    public float[] coverage;
    public float[] burnableFraction;
    public int bandSize;

    void setBa(double[] ba) {
        this.ba = ba;
    }

    void setPatchNumber(float[] patchNumber) {
        this.patchNumber = patchNumber;
    }

    void setErrors(float[] errors) {
        this.errors = errors;
    }

    void setBaInLc(List<double[]> baInLc) {
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
        out.writeInt(lcClassesCount);
        out.writeInt(bandSize);
        for (double v : ba) {
            out.writeDouble((int) v);
        }
        for (float v : patchNumber) {
            out.writeFloat(v);
        }
        for (float v : errors) {
            out.writeFloat((int) v);
        }
        for (double[] lcClass : baInLc) {
            for (double value : lcClass) {
//                out.writeFloat((int) value);
                out.writeDouble(value);
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
        lcClassesCount = in.readInt();
        int bandSize = in.readInt();
        ba = new double[bandSize];
        patchNumber = new float[bandSize];
        errors = new float[bandSize];
        baInLc = new ArrayList<>();
        for (int lcClass = 0; lcClass < lcClassesCount; lcClass++) {
            baInLc.add(new double[bandSize]);
        }
        coverage = new float[bandSize];
        burnableFraction = new float[bandSize];

        for (int i = 0; i < bandSize; i++) {
            ba[i] = in.readDouble();
        }
        for (int i = 0; i < bandSize; i++) {
            patchNumber[i] = in.readFloat();
        }
        for (int i = 0; i < bandSize; i++) {
            errors[i] = in.readFloat();
        }
        for (double[] lcClass : baInLc) {
            for (int i = 0; i < lcClass.length; i++) {
                lcClass[i] = in.readDouble();
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
        return "GridCells{" + "\n" +
                "ba=" + Arrays.toString(ba) + "\n" +
                ", patchNumber=" + Arrays.toString(patchNumber) + "\n" +
                ", errors=" + Arrays.toString(errors) + "\n" +
                ", baInLc=" + baInLc + "\n" +
                ", coverage=" + Arrays.toString(coverage) + "\n" +
                ", burnable=" + Arrays.toString(burnableFraction) + "\n" +
                '}';
    }
}
