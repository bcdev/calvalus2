package com.bc.calvalus.binning;

import java.util.Arrays;

/**
 * The {@code VectorImpl} class is a light-weight implementation of
 * the {@link WritableVector} interface. It operates on an array of {@code float}
 * elements. The array object is used by reference and is passed into the constructor.
 * The class is final for allowing method in-lining.
 *
 * @author Norman Fomferra
 */
public final class VectorImpl implements WritableVector {
    private final float[] array;
    private int offset;
    private int size;

    public VectorImpl(float[] array) {
        this.array = array;
        this.size = array.length;
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

    void setOffsetAndSize(int offset, int size) {
        this.offset = offset;
        this.size = size;
    }
}
