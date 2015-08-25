package com.bc.calvalus.processing.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * An input split that optionally carries a list of parameters
 * encoded as key1, value1, key2, value2 ... in a string array.
 * Used in conjunction with TableInputFormat.
 *
 * @author Martin Boettcher
 */
public class ParameterizedSplit extends FileSplit implements ProgressSplit {
    private float progress; // not serialized
    String[] parameters;

    public ParameterizedSplit() {}

    public ParameterizedSplit(Path path, long len, String[] hosts, String[] parameters) {
        super(path, 0, len, hosts);
        this.parameters = parameters;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(parameters.length);
        for (String parameter : parameters) {
            out.writeUTF(parameter);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        final int numParameters = in.readInt();
        parameters = new String[numParameters];
        for (int i=0; i<numParameters; ++i) {
            parameters[i] = in.readUTF();
        }
    }

    @Override
    public void setProgress(float progress) {
        this.progress = progress;
    }

    @Override
    public float getProgress() {
        return progress;
    }

    public String[] getParameters() {
        return parameters;
    }
}
