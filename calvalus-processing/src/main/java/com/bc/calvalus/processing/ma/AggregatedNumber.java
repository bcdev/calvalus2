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
     * N is the number of "good" values that have been used to compute min, max, mean and stdDev.
     * <ol>
     * <li>not {@code NaN},</li>
     * <li>not masked out by a user-provided valid-mask.</li>
     * </ol>
     */
    public final int n;

    /**
     * NT is the total number of values that were not NaN.
     * In the BEAM data model: the pixels whose "validMask" is set.
     */
    public final int nT;

    /**
     * NF is the number of values x that have been filtered out since they do not satisfy the condition
     * ({@link #mean} - a * {@link #stdDev}) < x <  ({@link #mean} + a * {@link #stdDev}), where a is most likely
     * 1.5.
     */
    public final int nF;

    /**
     * The minimum value of all "good" values (see {@link #n}).
     */
    public final double min;

    /**
     * The maximum value of all "good" values (see {@link #n}).
     */
    public final double max;

    /**
     * The mean of the "good" values (see {@link #n}).
     */
    public final double mean;

    /**
     * The standard deviation of the "good" values (see {@link #n}).
     */
    public final double stdDev;

    /**
     * The coefficient of variance is {@link #stdDev} / {@link #mean}.
     */
    public final double cv;

    public AggregatedNumber(int n, int nT, int nF,
                            double min,
                            double max,
                            double mean,
                            double stdDev) {
        this.nT = nT;
        this.min = min;
        this.max = max;
        this.n = n;
        this.nF = nF;
        this.mean = mean;
        this.stdDev = stdDev;
        this.cv = stdDev / mean;
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
