package com.bc.calvalus.b3;

import java.util.Arrays;

/**
 * The class is final for allowing method inlining.
 *
 * @author Norman Fomferra
 */
public final class VectorImpl implements WritableVector {
    private final float[] array;
    private int offset;
    private int size;

    public VectorImpl(float[] array) {
        this.array = array;
    }

    public void setOffsetAndSize(int offset, int size) {
        this.offset = offset;
        this.size = size;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public float get(int i) {
        return array[offset + i];
    }

    @Override
    public void set(int i, float v) {
        array[offset + i] = v;
    }

    @Override
    public String toString() {
        return Arrays.toString(Arrays.copyOfRange(array, offset, offset + size));
    }
}
