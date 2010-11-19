package com.bc.calvalus.binning;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * A spatial bin that is Hadoop-serializable.
 *
 * @author Marco Zuehlke
 * @author Norman Fomferra
 */
public class SpatialBin implements Bin<ObservationImpl>, Writable {
    static final boolean LOGN = false;

    int index;
    int numObs;
    float sumX;
    float sumXX;

    public SpatialBin() {
        this.index = -1;
    }

    public SpatialBin(int index) {
        this.index = index;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public void addObservation(ObservationImpl observation) {
        if (LOGN) {
            double x = Math.log(observation.getX());
            sumX += x;
            sumXX += x * x;
            numObs++;
        } else {
            double x = observation.getX();
            sumX += x;
            sumXX += x * x;
            numObs++;
        }
    }

    @Override
    public void close() {
        if (LOGN) {
            float weight = (float) Math.sqrt(numObs);
            sumX /= weight;
            sumXX /= weight;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(numObs);
        out.writeFloat(sumX);
        out.writeFloat(sumXX);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        numObs = in.readInt();
        sumX = in.readFloat();
        sumXX = in.readFloat();
    }

    public static SpatialBin read(DataInput in) throws IOException {
        SpatialBin w = new SpatialBin();
        w.readFields(in);
        return w;
    }

    @Override
    public String toString() {
        return "SpatialBin{" +
                "index=" + index +
                ", numObs=" + numObs +
                ", sumX=" + sumX +
                ", sumXX=" + sumXX +
                '}';
    }
}
