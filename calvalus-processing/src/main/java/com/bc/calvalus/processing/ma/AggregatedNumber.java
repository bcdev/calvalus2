/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.ma;

/**
 * Basically this class holds a number of statistics derived from an arry of numbers.
 * It is a {@link Number} so that its 'mean' value can be used as a numeric record value.
 *
 * @author MarcoZ
 * @author Norman
 */
public class AggregatedNumber extends Number {
    private final float sum;
    private final float mean;
    private final int numGoodPixels;
    private final int numTotalPixels;

    public AggregatedNumber(float sum, int numGoodPixels, int numTotalPixels) {
        this.sum = sum;
        this.mean = numGoodPixels > 0 ? sum / numGoodPixels : Float.NaN;
        this.numGoodPixels = numGoodPixels;
        this.numTotalPixels = numTotalPixels;
    }

    public float getSum() {
        return sum;
    }

    public float getMean() {
        return mean;
    }

    public int getNumGoodPixels() {
        return numGoodPixels;
    }

    public int getNumTotalPixels() {
        return numTotalPixels;
    }

    @Override
    public int intValue() {
        return Math.round(getMean());
    }

    @Override
    public long longValue() {
        return Math.round(getMean());
    }

    @Override
    public float floatValue() {
        return getMean();
    }

    @Override
    public double doubleValue() {
        return getMean();
    }

    @Override
    public String toString() {
        return String.valueOf(getMean());
    }
}
