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

package com.bc.calvalus.binning;


import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

import static java.lang.Float.NaN;
import static org.junit.Assert.*;

public class AggregatorSRTest {

    private VariableContext varCtx;
    private BinContext ctx;
    private AggregatorSR agg;

    @Before
    public void setUp() throws Exception {
        varCtx = new MyVariableContext("sdr_1", "sdr_2", "sdr_3",
                                       "sdr_error_1", "sdr_error_2", "sdr_error_3",
                                       "status");
        agg = new AggregatorSR(varCtx, 3, Float.NaN);
        ctx = new MyBinContext();
    }

    @Test
    public void testMetadata() {

        assertEquals("LC_SR", agg.getName());
        assertTrue(Float.isNaN(agg.getOutputFillValue()));

        String[] spatialFeatureNames = agg.getSpatialFeatureNames();
        assertEquals(11, spatialFeatureNames.length);
        assertEquals("land_count", spatialFeatureNames[0]);
        assertEquals("water_count", spatialFeatureNames[1]);
        assertEquals("snow_count", spatialFeatureNames[2]);
        assertEquals("cloud_count", spatialFeatureNames[3]);
        assertEquals("cloud_shadow_count", spatialFeatureNames[4]);
        assertEquals("sdr_1_sum_x", spatialFeatureNames[5]);
        assertEquals("sdr_2_sum_x", spatialFeatureNames[6]);
        assertEquals("sdr_3_sum_x", spatialFeatureNames[7]);
        assertEquals("sdr_error_1_sum_xx", spatialFeatureNames[8]);
        assertEquals("sdr_error_2_sum_xx", spatialFeatureNames[9]);
        assertEquals("sdr_error_3_sum_xx", spatialFeatureNames[10]);

        String[] temporalFeatureNames = agg.getTemporalFeatureNames();
        assertEquals(11, temporalFeatureNames.length);
        assertArrayEquals(spatialFeatureNames, temporalFeatureNames);

        String[] outputFeatureNames = agg.getOutputFeatureNames();
        assertEquals(11, outputFeatureNames.length);
        assertArrayEquals(spatialFeatureNames, temporalFeatureNames);
    }

    @Test
    public void testSpatialBinning_WithoutSnow() {
        VectorImpl svec = vec(11, NaN);
        agg.initSpatial(ctx, svec);
        assertEquals(0.0f, svec.get(0), 0.0f);
        assertEquals(0.0f, svec.get(1), 0.0f);
        assertEquals(0.0f, svec.get(2), 0.0f);
        assertEquals(0.0f, svec.get(3), 0.0f);
        assertEquals(0.0f, svec.get(4), 0.0f);
        assertEquals(0.0f, svec.get(5), 0.0f);
        assertEquals(0.0f, svec.get(6), 0.0f);
        assertEquals(0.0f, svec.get(7), 0.0f);
        assertEquals(0.0f, svec.get(8), 0.0f);
        assertEquals(0.0f, svec.get(9), 0.0f);
        assertEquals(0.0f, svec.get(10), 0.0f);

        agg.aggregateSpatial(ctx, vec(1.1f, 1.3f, 1.5f, 0.1f, 0.3f, 0.5f, 1), svec);
        agg.aggregateSpatial(ctx, vec(1.1f, 1.3f, 1.5f, 0.1f, 0.3f, 0.5f, 2), svec);
        agg.aggregateSpatial(ctx, vec(1.1f, 1.3f, 1.5f, 0.1f, 0.3f, 0.5f, 1), svec);

        assertEquals(2f, svec.get(0), 1e-5f);
        assertEquals(1f, svec.get(1), 1e-5f);
        assertEquals(0f, svec.get(2), 1e-5f);
        assertEquals(0f, svec.get(3), 1e-5f);
        assertEquals(0f, svec.get(4), 1e-5f);

        assertEquals(1.1f + 1.1f, svec.get(5), 1e-5f);
        assertEquals(1.3f + 1.3f, svec.get(6), 1e-5f);
        assertEquals(1.5f + 1.5f, svec.get(7), 1e-5f);

        assertEquals((0.1f * 0.1f) + (0.1f * 0.1f), svec.get(8), 1e-5f);
        assertEquals((0.3f * 0.3f) + (0.3f * 0.3f), svec.get(9), 1e-5f);
        assertEquals((0.5f * 0.5f) + (0.5f * 0.5f), svec.get(10), 1e-5f);
    }

   @Test
    public void testSpatialBinning_WithSnowAndLand() {
        VectorImpl svec = vec(11, NaN);
        agg.initSpatial(ctx, svec);
        assertEquals(0.0f, svec.get(0), 0.0f);
        assertEquals(0.0f, svec.get(1), 0.0f);
        assertEquals(0.0f, svec.get(2), 0.0f);
        assertEquals(0.0f, svec.get(3), 0.0f);
        assertEquals(0.0f, svec.get(4), 0.0f);
        assertEquals(0.0f, svec.get(5), 0.0f);
        assertEquals(0.0f, svec.get(6), 0.0f);
        assertEquals(0.0f, svec.get(7), 0.0f);
        assertEquals(0.0f, svec.get(8), 0.0f);
        assertEquals(0.0f, svec.get(9), 0.0f);
        assertEquals(0.0f, svec.get(10), 0.0f);

        agg.aggregateSpatial(ctx, vec(1.1f, 1.3f, 1.5f, 0.1f, 0.3f, 0.5f, 3), svec);
        agg.aggregateSpatial(ctx, vec(1.1f, 1.3f, 1.5f, 0.1f, 0.3f, 0.5f, 2), svec);
        agg.aggregateSpatial(ctx, vec(1.1f, 1.3f, 1.5f, 0.1f, 0.3f, 0.5f, 4), svec);
        agg.aggregateSpatial(ctx, vec(1.1f, 1.3f, 1.5f, 0.1f, 0.3f, 0.5f, 1), svec);
        agg.aggregateSpatial(ctx, vec(1.1f, 1.3f, 1.5f, 0.1f, 0.3f, 0.5f, 1), svec);

        assertEquals(2f, svec.get(0), 1e-5f);
        assertEquals(1f, svec.get(1), 1e-5f);
        assertEquals(1f, svec.get(2), 1e-5f);
        assertEquals(1f, svec.get(3), 1e-5f);
        assertEquals(0f, svec.get(4), 1e-5f);

        assertEquals(1.1f + 1.1f, svec.get(5), 1e-5f);
        assertEquals(1.3f + 1.3f, svec.get(6), 1e-5f);
        assertEquals(1.5f + 1.5f, svec.get(7), 1e-5f);

       assertEquals((0.1f * 0.1f) + (0.1f * 0.1f), svec.get(8), 1e-5f);
       assertEquals((0.3f * 0.3f) + (0.3f * 0.3f), svec.get(9), 1e-5f);
       assertEquals((0.5f * 0.5f) + (0.5f * 0.5f), svec.get(10), 1e-5f);
    }

    @Test
     public void testSpatialBinning_WithSnowOnly() {
         VectorImpl svec = vec(11, NaN);
         agg.initSpatial(ctx, svec);
         assertEquals(0.0f, svec.get(0), 0.0f);
         assertEquals(0.0f, svec.get(1), 0.0f);
         assertEquals(0.0f, svec.get(2), 0.0f);
         assertEquals(0.0f, svec.get(3), 0.0f);
         assertEquals(0.0f, svec.get(4), 0.0f);
         assertEquals(0.0f, svec.get(5), 0.0f);
         assertEquals(0.0f, svec.get(6), 0.0f);
         assertEquals(0.0f, svec.get(7), 0.0f);
         assertEquals(0.0f, svec.get(8), 0.0f);
         assertEquals(0.0f, svec.get(9), 0.0f);
         assertEquals(0.0f, svec.get(10), 0.0f);

         agg.aggregateSpatial(ctx, vec(1.1f, 1.3f, 1.5f, 0.1f, 0.3f, 0.5f, 3), svec);
         agg.aggregateSpatial(ctx, vec(1.1f, 1.3f, 1.5f, 0.1f, 0.3f, 0.5f, 3), svec);
         agg.aggregateSpatial(ctx, vec(1.1f, 1.3f, 1.5f, 0.1f, 0.3f, 0.5f, 4), svec);

         assertEquals(0f, svec.get(0), 1e-5f);
         assertEquals(0f, svec.get(1), 1e-5f);
         assertEquals(2f, svec.get(2), 1e-5f);
         assertEquals(1f, svec.get(3), 1e-5f);
         assertEquals(0f, svec.get(4), 1e-5f);

         assertEquals(1.1f + 1.1f, svec.get(5), 1e-5f);
         assertEquals(1.3f + 1.3f, svec.get(6), 1e-5f);
         assertEquals(1.5f + 1.5f, svec.get(7), 1e-5f);

        assertEquals((0.1f * 0.1f) + (0.1f * 0.1f), svec.get(8), 1e-5f);
        assertEquals((0.3f * 0.3f) + (0.3f * 0.3f), svec.get(9), 1e-5f);
        assertEquals((0.5f * 0.5f) + (0.5f * 0.5f), svec.get(10), 1e-5f);
      }

    private static VectorImpl vec(int count, float value) {
        float[] values = new float[count];
        Arrays.fill(values, value);
        return new VectorImpl(values);
    }

    private static VectorImpl vec(float... values) {
        return new VectorImpl(values);
    }

    private static class MyVariableContext implements VariableContext {
        private String[] varNames;

        public MyVariableContext(String... varNames) {
            this.varNames = varNames;
        }

        @Override
        public int getVariableCount() {
            return varNames.length;
        }

        @Override
        public String getVariableName(int i) {
            return varNames[i];
        }

        @Override
        public int getVariableIndex(String varName) {
            for (int i = 0; i < varNames.length; i++) {
                if (varName.equals(varNames[i])) {
                    return i;
                }
            }
            return -1;
        }

        @Override
        public String getVariableExpr(int i) {
            return null;
        }


        @Override
        public String getMaskExpr() {
            return null;
        }
    }

    private static class MyBinContext implements BinContext {
        private HashMap map = new HashMap();

        @Override
        public long getIndex() {
            return 0;
        }

        @Override
        public <T> T get(String name) {
            return (T) map.get(name);
        }

        @Override
        public void put(String name, Object value) {
            map.put(name, value);
        }
    }
}
