package com.bc.calvalus.binning;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;


/**
 * A Hadoop-serializable bin.
 *
 * @author Norman Fomferra
 */
public abstract class Bin implements BinContext, Writable {

    long index;
    int numObs;
    float[] properties;

    // Not serialized for Hadoop
    private transient HashMap<String, Object> contextMap;

    public Bin() {
        this.index = -1;
    }

    public Bin(long index, int numProperties) {
        if (numProperties < 0) {
            throw new IllegalArgumentException("numProperties < 0");
        }
        this.index = index;
        this.properties = new float[numProperties];
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public int getNumObs() {
        return numObs;
    }

    public void setNumObs(int numObs) {
        this.numObs = numObs;
    }

    public int getPropertyCount() {
        return properties.length;
    }

    public float getProperty(int i) {
        return properties[i];
    }

    public float[] getProperties() {
        return properties;
    }

    @Override
    public <T> T get(String name) {
        return contextMap != null ? (T) contextMap.get(name) : null;
    }

    @Override
    public void put(String name, Object value) {
        if (contextMap == null) {
            contextMap = new HashMap<String, Object>();
        }
        contextMap.put(name, value);
    }
}
