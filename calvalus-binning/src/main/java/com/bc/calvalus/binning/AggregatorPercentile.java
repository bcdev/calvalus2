/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.binning;

import java.util.Arrays;

/**
 * An aggregator that computes the p-th percentile,
 * the value of a variable below which a certain percent (p) of observations fall.
 *
 * @author MarcoZ
 * @author Norman
 */
public class AggregatorPercentile implements Aggregator {
    private final int varIndex;
    private final String[] spatialPropertyNames;
    private final String[] temporalPropertyNames;
    private final int percentage;
    private final double fillValue;

    public AggregatorPercentile(VariableContext varCtx, String varName, Integer percentage, Double fillValue) {
        if (varCtx == null) {
            throw new NullPointerException("varCtx");
        }
        if (varName == null) {
            throw new NullPointerException("varName");
        }
        if (percentage != null && (percentage < 0 || percentage > 100)) {
            throw new IllegalArgumentException("percentage < 0 || percentage > 100");
        }
        this.varIndex = varCtx.getVariableIndex(varName);
        this.spatialPropertyNames = new String[]{varName + "_sum_x"};
        this.temporalPropertyNames = new String[]{varName + "_P" + percentage};
        this.percentage = percentage != null ? percentage : 90;
        this.fillValue = fillValue != null ? fillValue : Double.NaN;
    }

    @Override
    public String getName() {
        return "PERCENTILE";
    }

    @Override
    public int getSpatialPropertyCount() {
        return spatialPropertyNames.length;
    }

    @Override
    public String getSpatialPropertyName(int i) {
        return spatialPropertyNames[i];
    }

    @Override
    public int getTemporalPropertyCount() {
        return temporalPropertyNames.length;
    }

    @Override
    public String getTemporalPropertyName(int i) {
        return temporalPropertyNames[i];
    }

    @Override
    public int getOutputPropertyCount() {
        return temporalPropertyNames.length;
    }

    @Override
    public String getOutputPropertyName(int i) {
        return temporalPropertyNames[i];
    }

    @Override
    public double getOutputPropertyFillValue(int i) {
        return fillValue;
    }

    @Override
    public void initSpatial(BinContext ctx, WritableVector vector) {
        vector.set(0, 0.0f);
    }

    @Override
    public void aggregateSpatial(BinContext ctx, Vector observationVector, WritableVector spatialVector) {
        final float value = observationVector.get(varIndex);
        spatialVector.set(0, spatialVector.get(0) + value);
    }

    @Override
    public void completeSpatial(BinContext ctx, int numSpatialObs, WritableVector spatialVector) {
        spatialVector.set(0, spatialVector.get(0) / numSpatialObs);
    }

    @Override
    public void initTemporal(BinContext ctx, WritableVector vector) {
        vector.set(0, 0.0f);
        ctx.put("ml", new GrowableVector(256));
    }

    @Override
    public void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs, WritableVector temporalVector) {
        GrowableVector measurementsVec = ctx.get("ml");
        measurementsVec.add(spatialVector.get(0));
    }

    @Override
    public void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector) {
        GrowableVector measurementsVec = ctx.get("ml");
        float[] measurements = measurementsVec.getElements();
        Arrays.sort(measurements);
        temporalVector.set(0, computePercentile(percentage, measurements));
    }


    @Override
    public void computeOutput(Vector temporalVector, WritableVector outputVector) {
        outputVector.set(0, temporalVector.get(0));
    }

    @Override
    public String toString() {
        return "AggregatorPercentile{" +
                "varIndex=" + varIndex +
                ", percentage=" + percentage +
                ", spatialPropertyNames=" + (spatialPropertyNames == null ? null : Arrays.toString(spatialPropertyNames)) +
                ", temporalPropertyNames=" + (temporalPropertyNames == null ? null : Arrays.toString(temporalPropertyNames)) +
                '}';
    }

    /**
     * Computes the p-th percentile of an array of measurements following
     * the "Engineering Statistics Handbook: Percentile". NIST.
     * http://www.itl.nist.gov/div898/handbook/prc/section2/prc252.htm.
     * Retrieved 2011-03-16.
     *
     * @param p            The percentage in percent ranging from 0 to 100.
     * @param measurements Sorted array of measurements.
     * @return The  p-th percentile.
     */
    public static float computePercentile(int p, float[] measurements) {
        int N = measurements.length;
        float n = (p / 100.0F) * (N + 1);
        int k = (int) Math.floor(n);
        float d = n - k;
        float yp;
        if (k == 0) {
            yp = measurements[0];
        } else if (k >= N) {
            yp = measurements[N - 1];
        } else {
            yp = measurements[k - 1] + d * (measurements[k] - measurements[k - 1]);
        }
        return yp;
    }
}
