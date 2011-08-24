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
 * The class satisfies the {@link Number} contract by providing its 'mean' value.
 *
 * @author MarcoZ
 * @author Norman
 */
public final class AggregatedNumber extends Number {

    /**
     * The number of total pixels is the number of values that are not NaN.
     * In the BEAM data model: the pixels whose "validMask" is set.
     */
    public final int numTotalPixels;

    /**
     * The minimum value of all good pixels (see {@link #numGoodPixels}).
     */
    public final double min;

    /**
     * The maximum value of all good pixels (see {@link #numGoodPixels}).
     */
    public final double max;

    /**
     * The mean of the good pixels (see {@link #numGoodPixels}).
     */
    public final double mean;

    /**
     * The standard deviation of the good pixels (see {@link #numGoodPixels}).
     */
    public final double stdDev;

    /**
     * The number of good pixels is the number of values that are
     * <ol>
     * <li>not {@code NaN},</li>
     * <li>not masked out by a user-provided valid-mask.</li>
     * </ol>
     */
    public final int numGoodPixels;

    /**
     * The mean of filtered values (see {@link #numFilteredPixels}).
     */
    public final double filteredMean;

    /**
     * The mean of filtered values (see {@link #numFilteredPixels}).
     */
    public final double filteredStdDev;

    /**
     * The coefficient of variance is {@link #filteredStdDev} / {@link #filteredMean}
     */
    public final double cv;

    /**
     * The number of filtered values is the number of values x satisfying ({@link #mean} - a * {@link #stdDev}) < x <  ({@link #mean} + a * {@link #stdDev}).
     */
    public final int numFilteredPixels;

    public AggregatedNumber(int numTotalPixels,
                            double min,
                            double max,
                            double mean,
                            double stdDev,
                            int numGoodPixels,
                            double filteredMean,
                            double filteredStdDev,
                            int numFilteredPixels) {
        this.numTotalPixels = numTotalPixels;
        this.min = min;
        this.max = max;
        this.numGoodPixels = numGoodPixels;
        this.numFilteredPixels = numFilteredPixels;
        this.mean = mean;
        this.stdDev = stdDev;
        this.filteredMean = filteredMean;
        this.filteredStdDev = filteredStdDev;
        this.cv = filteredStdDev / filteredMean;
    }

    @Override
    public int intValue() {
        return (int) Math.round(mean);
    }

    @Override
    public long longValue() {
        return Math.round(mean);
    }

    @Override
    public float floatValue() {
        return (float) mean;
    }

    @Override
    public double doubleValue() {
        return mean;
    }

    @Override
    public String toString() {
        return String.valueOf(floatValue());
    }
}
