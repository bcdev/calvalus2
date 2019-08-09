/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * A numerical value range.
 *
 * @author MarcoZ
 */
public class DtoValueRange implements IsSerializable {

    private double min;
    private double max;
    private boolean minIncluded;
    private boolean maxIncluded;

    public DtoValueRange() {
    }

    public DtoValueRange(double min, double max, boolean minIncluded, boolean maxIncluded) {
        this.min = min;
        this.max = max;
        this.minIncluded = minIncluded;
        this.maxIncluded = maxIncluded;
    }

    public boolean contains(double v) {
        boolean b1 = minIncluded ? (v >= min) : (v > min);
        boolean b2 = maxIncluded ? (v <= max) : (v < max);
        return b1 && b2;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(minIncluded ? '[' : '(');
        sb.append(hasMin() ? toString(min) : "*");
        sb.append(',');
        sb.append(hasMax() ? toString(max) : "*");
        sb.append(maxIncluded ? ']' : ')');
        return sb.toString();
    }

    private boolean hasMin() {
        return min > Double.NEGATIVE_INFINITY;
    }

    private boolean hasMax() {
        return max < Double.POSITIVE_INFINITY;
    }


    private static String toString(double d) {
        final long l = Math.round(d);
        return d == l ? Long.toString(l) : Double.toString(d);
    }
}
