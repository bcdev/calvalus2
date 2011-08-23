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
public final class AggregatedNumber extends Number {
    public final int numTotalPixels;
    public final int numGoodPixels;
    public final int numFilteredPixels;
    public final float mean;
    public final float stdDev;
    public final float filteredMean;
    public final float filteredStdDev;
    public final float CV;

    public AggregatedNumber(int numTotalPixels,
                            int numGoodPixels,
                            int numFilteredPixels,
                            float mean,
                            float stdDev,
                            float filteredMean,
                            float filteredStdDev) {
        this.numTotalPixels = numTotalPixels;
        this.numGoodPixels = numGoodPixels;
        this.numFilteredPixels = numFilteredPixels;
        this.mean = mean;
        this.stdDev = stdDev;
        this.filteredMean = filteredMean;
        this.filteredStdDev = filteredStdDev;
        this.CV = filteredStdDev / filteredMean;
    }

    @Override
    public int intValue() {
        return Math.round(mean);
    }

    @Override
    public long longValue() {
        return Math.round(mean);
    }

    @Override
    public float floatValue() {
        return mean;
    }

    @Override
    public double doubleValue() {
        return mean;
    }

    @Override
    public String toString() {
        return String.valueOf(mean);
    }
}
