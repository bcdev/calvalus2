package com.bc.calvalus.binning;

/**
 * A bin in which pixel values are collected.
 * @param <PIXEL>
 */
public abstract class AbstractBin<PIXEL extends Observation> implements Bin<PIXEL> {
    private final int index;

    public AbstractBin(int index) {
        this.index = index;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractBin)) {
            return false;
        }

        AbstractBin bin = (AbstractBin) o;

        return index == bin.index;
    }

    @Override
    public int hashCode() {
        return index;
    }

}
