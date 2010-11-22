package com.bc.calvalus.binning;

/**
 * A bin in which observations (a pixel's sample values) are collected.
 * @param <OBS> The observation type.
 */
public abstract class AbstractBin<OBS extends Observation> implements Bin<OBS> {
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
