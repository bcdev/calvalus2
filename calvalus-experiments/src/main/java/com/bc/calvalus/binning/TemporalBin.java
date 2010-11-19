package com.bc.calvalus.binning;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Writable for bins.
 *
 * @author Marco Zuehlke
 * @author Norman Fomferra
 */
public class TemporalBin extends SpatialBin {
    float weight;
    int numPasses;

    public TemporalBin() {
        super();
    }

    public TemporalBin(int index) {
        super(index);
    }

    public void addBin(SpatialBin bin) {
        sumX += bin.sumX;
        sumXX += bin.sumXX;
        numObs += bin.numObs;
        weight += Math.sqrt(bin.numObs);
        numPasses++;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeFloat(weight);
        out.writeInt(numPasses);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        weight = in.readFloat();
        numPasses = in.readInt();
    }

    public static TemporalBin read(DataInput in) throws IOException {
        TemporalBin w = new TemporalBin();
        w.readFields(in);
        return w;
    }

    @Override
    public String toString() {
        return "TemporalBin{" +
                "index=" + index +
                ", numObs=" + numObs +
                ", numPasses=" + numPasses +
                ", sumX=" + sumX +
                ", sumXX=" + sumXX +
                ", weight=" + weight +
                '}';
    }
}
