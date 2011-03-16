package com.bc.calvalus.binning;

import java.util.Arrays;

/**
 * A growable vector.
 *
 * @author Norman Fomferra
 */
public final class GrowableVector implements Vector {
    private float[] elements;
    private int size;

    public GrowableVector(int capacity) {
        this.elements = new float[capacity];
        this.size = 0;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public float get(int index) {
        return elements[index];
    }

    public float[] getElements() {
        return Arrays.copyOfRange(elements, 0, size);
    }

    public void add(float element) {
        if (size >= elements.length) {
            float[] temp = new float[(elements.length * 3) / 2 + 2];
            System.arraycopy(elements, 0, temp, 0, size);
            elements = temp;
        }
        elements[size++] = element;
    }

    @Override
    public String toString() {
        return Arrays.toString(getElements());
    }

}
