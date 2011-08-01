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
import static java.lang.Math.sqrt;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class AggregatorSRTest {

    private VariableContext varCtx;
    private BinContext ctx;
    private AggregatorSR agg;

    @Before
    public void setUp() throws Exception {
        varCtx = new MyVariableContext("sdr_1", "sdr_2", "sdr_3",
                                       "ndvi",
                                       "sdr_error_1", "sdr_error_2", "sdr_error_3",
                                       "status", "x", "y");
        agg = new AggregatorSR(varCtx, 3, Float.NaN);
        ctx = new MyBinContext();
    }

    @Test
    public void testMetadata() {

        assertEquals("LC_SR", agg.getName());
        assertTrue(Float.isNaN(agg.getOutputFillValue()));

        String[] spatialFeatureNames = agg.getSpatialFeatureNames();
        assertEquals(17, spatialFeatureNames.length);
        assertEquals("land_area", spatialFeatureNames[0]);
        assertEquals("water_area", spatialFeatureNames[1]);
        assertEquals("snow_area", spatialFeatureNames[2]);
        assertEquals("cloud_area", spatialFeatureNames[3]);
        assertEquals("cloud_shadow_area", spatialFeatureNames[4]);

        assertEquals("land_count", spatialFeatureNames[5]);
        assertEquals("water_count", spatialFeatureNames[6]);
        assertEquals("snow_count", spatialFeatureNames[7]);
        assertEquals("cloud_count", spatialFeatureNames[8]);
        assertEquals("cloud_shadow_count", spatialFeatureNames[9]);

        assertEquals("sdr_1_sum_x", spatialFeatureNames[10]);
        assertEquals("sdr_2_sum_x", spatialFeatureNames[11]);
        assertEquals("sdr_3_sum_x", spatialFeatureNames[12]);
        assertEquals("ndvi_sum_x", spatialFeatureNames[13]);
        assertEquals("sdr_error_1_sum_xx", spatialFeatureNames[14]);
        assertEquals("sdr_error_2_sum_xx", spatialFeatureNames[15]);
        assertEquals("sdr_error_3_sum_xx", spatialFeatureNames[16]);

        String[] temporalFeatureNames = agg.getTemporalFeatureNames();
        assertEquals(19, temporalFeatureNames.length);
        assertEquals("land_area", spatialFeatureNames[0]);
        assertEquals("water_area", spatialFeatureNames[1]);
        assertEquals("snow_area", spatialFeatureNames[2]);
        assertEquals("cloud_area", spatialFeatureNames[3]);
        assertEquals("cloud_shadow_area", spatialFeatureNames[4]);

        assertEquals("land_count", spatialFeatureNames[5]);
        assertEquals("water_count", spatialFeatureNames[6]);
        assertEquals("snow_count", spatialFeatureNames[7]);
        assertEquals("cloud_count", spatialFeatureNames[8]);
        assertEquals("cloud_shadow_count", spatialFeatureNames[9]);

        assertEquals("status", temporalFeatureNames[10]);
        assertEquals("w_sum", temporalFeatureNames[11]);

        assertEquals("sdr_1_sum_x", temporalFeatureNames[12]);
        assertEquals("sdr_2_sum_x", temporalFeatureNames[13]);
        assertEquals("sdr_3_sum_x", temporalFeatureNames[14]);
        assertEquals("ndvi_sum_x", temporalFeatureNames[15]);
        assertEquals("sdr_error_1_sum_xx", temporalFeatureNames[16]);
        assertEquals("sdr_error_2_sum_xx", temporalFeatureNames[17]);
        assertEquals("sdr_error_3_sum_xx", temporalFeatureNames[18]);

        String[] outputFeatureNames = agg.getOutputFeatureNames();
        assertEquals(18, outputFeatureNames.length);
        assertEquals("land_area", spatialFeatureNames[0]);
        assertEquals("water_area", spatialFeatureNames[1]);
        assertEquals("snow_area", spatialFeatureNames[2]);
        assertEquals("cloud_area", spatialFeatureNames[3]);
        assertEquals("cloud_shadow_area", spatialFeatureNames[4]);

        assertEquals("land_count", spatialFeatureNames[5]);
        assertEquals("water_count", spatialFeatureNames[6]);
        assertEquals("snow_count", spatialFeatureNames[7]);
        assertEquals("cloud_count", spatialFeatureNames[8]);
        assertEquals("cloud_shadow_count", spatialFeatureNames[9]);

        assertEquals("status", outputFeatureNames[10]);

        assertEquals("sr_1_mean", outputFeatureNames[11]);
        assertEquals("sr_2_mean", outputFeatureNames[12]);
        assertEquals("sr_3_mean", outputFeatureNames[13]);
        assertEquals("ndvi_mean", outputFeatureNames[14]);
        assertEquals("sr_1_sigma", outputFeatureNames[15]);
        assertEquals("sr_2_sigma", outputFeatureNames[16]);
        assertEquals("sr_3_sigma", outputFeatureNames[17]);
    }

    @Test
    public void testSpatialBinning() {
        VectorImpl svec = vec(17, NaN);
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
        assertEquals(0.0f, svec.get(11), 0.0f);
        assertEquals(0.0f, svec.get(12), 0.0f);
        assertEquals(0.0f, svec.get(13), 0.0f);
        assertEquals(0.0f, svec.get(14), 0.0f);
        assertEquals(0.0f, svec.get(15), 0.0f);
        assertEquals(0.0f, svec.get(16), 0.0f);

        agg.aggregateSpatial(ctx, vec(1.1f, 1.3f, 1.5f, 0.5f, 0.1f, 0.3f, 0.5f, 2, 1, 1), svec);
        assertEquals(0f, svec.get(0), 1e-5f);
        assertEquals(1f, svec.get(1), 1e-5f);
        assertEquals(0f, svec.get(2), 1e-5f);
        assertEquals(0f, svec.get(3), 1e-5f);
        assertEquals(0f, svec.get(4), 1e-5f);

        assertEquals(0f, svec.get(5), 1e-5f);
        assertEquals(0f, svec.get(6), 1e-5f);
        assertEquals(0f, svec.get(7), 1e-5f);
        assertEquals(0f, svec.get(8), 1e-5f);
        assertEquals(0f, svec.get(9), 1e-5f);

        assertEquals(0.0f, svec.get(10), 0.0f);
        assertEquals(0.0f, svec.get(11), 0.0f);
        assertEquals(0.0f, svec.get(12), 0.0f);
        assertEquals(0.0f, svec.get(13), 0.0f);
        assertEquals(0.0f, svec.get(14), 0.0f);
        assertEquals(0.0f, svec.get(15), 0.0f);
        assertEquals(0.0f, svec.get(16), 0.0f);

        agg.aggregateSpatial(ctx, vec(2.1f, 2.3f, 2.5f, 0.5f, 0.1f, 0.3f, 0.5f, 3, 1, 1), svec);
        assertEquals(0f, svec.get(0), 1e-5f);
        assertEquals(1f, svec.get(1), 1e-5f);
        assertEquals(1f, svec.get(2), 1e-5f);
        assertEquals(0f, svec.get(3), 1e-5f);
        assertEquals(0f, svec.get(4), 1e-5f);

        assertEquals(0f, svec.get(5), 1e-5f);
        assertEquals(0f, svec.get(6), 1e-5f);
        assertEquals(0f, svec.get(7), 1e-5f);
        assertEquals(0f, svec.get(8), 1e-5f);
        assertEquals(0f, svec.get(9), 1e-5f);

        assertEquals(2.1f, svec.get(10), 1e-5f);
        assertEquals(2.3f, svec.get(11), 1e-5f);
        assertEquals(2.5f, svec.get(12), 1e-5f);
        assertEquals(0.5f, svec.get(13), 1e-5f);
        assertEquals(0.1f * 0.1f, svec.get(14), 1e-5f);
        assertEquals(0.3f * 0.3f, svec.get(15), 1e-5f);
        assertEquals(0.5f * 0.5f, svec.get(16), 1e-5f);

        agg.aggregateSpatial(ctx, vec(1.1f, 1.3f, 1.5f, 0.5f, 0.1f, 0.3f, 0.5f, 4, 1, 1), svec);
        assertEquals(0f, svec.get(0), 1e-5f);
        assertEquals(1f, svec.get(1), 1e-5f);
        assertEquals(1f, svec.get(2), 1e-5f);
        assertEquals(1f, svec.get(3), 1e-5f);
        assertEquals(0f, svec.get(4), 1e-5f);

        assertEquals(0f, svec.get(5), 1e-5f);
        assertEquals(0f, svec.get(6), 1e-5f);
        assertEquals(0f, svec.get(7), 1e-5f);
        assertEquals(0f, svec.get(8), 1e-5f);
        assertEquals(0f, svec.get(9), 1e-5f);

        assertEquals(2.1f, svec.get(10), 1e-5f);
        assertEquals(2.3f, svec.get(11), 1e-5f);
        assertEquals(2.5f, svec.get(12), 1e-5f);
        assertEquals(0.5f, svec.get(13), 1e-5f);
        assertEquals(0.1f * 0.1f, svec.get(14), 1e-5f);
        assertEquals(0.3f * 0.3f, svec.get(15), 1e-5f);
        assertEquals(0.5f * 0.5f, svec.get(16), 1e-5f);

        agg.aggregateSpatial(ctx, vec(1.1f, 1.3f, 1.5f, 0.5f, 0.1f, 0.3f, 0.5f, 1, 1, 1), svec);
        agg.aggregateSpatial(ctx, vec(1.1f, 1.3f, 1.5f, 0.5f, 0.1f, 0.3f, 0.5f, 1, 1, 1), svec);
        assertEquals(2f, svec.get(0), 1e-5f);
        assertEquals(1f, svec.get(1), 1e-5f);
        assertEquals(1f, svec.get(2), 1e-5f);
        assertEquals(1f, svec.get(3), 1e-5f);
        assertEquals(0f, svec.get(4), 1e-5f);

        assertEquals(0f, svec.get(5), 1e-5f);
        assertEquals(0f, svec.get(6), 1e-5f);
        assertEquals(0f, svec.get(7), 1e-5f);
        assertEquals(0f, svec.get(8), 1e-5f);
        assertEquals(0f, svec.get(9), 1e-5f);

        assertEquals(1.1f + 1.1f, svec.get(10), 1e-5f);
        assertEquals(1.3f + 1.3f, svec.get(11), 1e-5f);
        assertEquals(1.5f + 1.5f, svec.get(12), 1e-5f);
        assertEquals(1.0f, svec.get(13), 1e-5f);
        assertEquals((0.1f * 0.1f) + (0.1f * 0.1f), svec.get(14), 1e-5f);
        assertEquals((0.3f * 0.3f) + (0.3f * 0.3f), svec.get(15), 1e-5f);
        assertEquals((0.5f * 0.5f) + (0.5f * 0.5f), svec.get(16), 1e-5f);

        agg.completeSpatial(ctx, 5, svec);
        assertEquals(2f, svec.get(0), 1e-5f);
        assertEquals(1f, svec.get(1), 1e-5f);
        assertEquals(1f, svec.get(2), 1e-5f);
        assertEquals(1f, svec.get(3), 1e-5f);
        assertEquals(0f, svec.get(4), 1e-5f);

        assertEquals(1f, svec.get(5), 1e-5f);
        assertEquals(1f, svec.get(6), 1e-5f);
        assertEquals(1f, svec.get(7), 1e-5f);
        assertEquals(1f, svec.get(8), 1e-5f);
        assertEquals(0f, svec.get(9), 1e-5f);

        assertEquals(1.1f + 1.1f, svec.get(10), 1e-5f);
        assertEquals(1.3f + 1.3f, svec.get(11), 1e-5f);
        assertEquals(1.5f + 1.5f, svec.get(12), 1e-5f);
        assertEquals(1.0f, svec.get(13), 1e-5f);
        assertEquals((0.1f * 0.1f) + (0.1f * 0.1f), svec.get(14), 1e-5f);
        assertEquals((0.3f * 0.3f) + (0.3f * 0.3f), svec.get(15), 1e-5f);
        assertEquals((0.5f * 0.5f) + (0.5f * 0.5f), svec.get(16), 1e-5f);
    }

    @Test
    public void testTemporalBinning() {
        VectorImpl tvec = vec(19, NaN);
        agg.initTemporal(ctx, tvec);
        assertEquals(0.0f, tvec.get(0), 0.0f);
        assertEquals(0.0f, tvec.get(1), 0.0f);
        assertEquals(0.0f, tvec.get(2), 0.0f);
        assertEquals(0.0f, tvec.get(3), 0.0f);
        assertEquals(0.0f, tvec.get(4), 0.0f);

        assertEquals(0.0f, tvec.get(5), 0.0f);
        assertEquals(0.0f, tvec.get(6), 0.0f);
        assertEquals(0.0f, tvec.get(7), 0.0f);
        assertEquals(0.0f, tvec.get(8), 0.0f);
        assertEquals(0.0f, tvec.get(9), 0.0f);

        assertEquals(0.0f, tvec.get(10), 0.0f);
        assertEquals(0.0f, tvec.get(11), 0.0f);

        assertEquals(0.0f, tvec.get(12), 0.0f);
        assertEquals(0.0f, tvec.get(13), 0.0f);
        assertEquals(0.0f, tvec.get(14), 0.0f);

        assertEquals(0.0f, tvec.get(15), 0.0f);

        assertEquals(0.0f, tvec.get(16), 0.0f);
        assertEquals(0.0f, tvec.get(17), 0.0f);
        assertEquals(0.0f, tvec.get(18), 0.0f);

        // 2 land 1 snow
        agg.aggregateTemporal(ctx, vec(2f, 1f, 1f, 3f, 2f,
                                       1f, 1f, 1f, 1f, 1f,
                                       1.1f, 1.3f, 1.5f,
                                       0.5f,
                                       0.1f, 0.3f, 0.5f), 9, tvec);
        assertEquals(2.0f / 9f, tvec.get(0), 0.0f);
        assertEquals(1.0f / 9f, tvec.get(1), 0.0f);
        assertEquals(1.0f / 9f, tvec.get(2), 0.0f);
        assertEquals(3.0f / 9f, tvec.get(3), 0.0f);
        assertEquals(2.0f / 9f, tvec.get(4), 0.0f);

        assertEquals(1.0f, tvec.get(5), 0.0f);
        assertEquals(1.0f, tvec.get(6), 0.0f);
        assertEquals(1.0f, tvec.get(7), 0.0f);
        assertEquals(1.0f, tvec.get(8), 0.0f);
        assertEquals(1.0f, tvec.get(9), 0.0f);

        assertEquals(1.0f, tvec.get(10), 0.0f);
        assertEquals(1f / sqrt(2), tvec.get(11), 1e-5f);

        assertEquals(1.1f / sqrt(2) / 2, tvec.get(12), 1e-5f);
        assertEquals(1.3f / sqrt(2) / 2, tvec.get(13), 1e-5f);
        assertEquals(1.5f / sqrt(2) / 2, tvec.get(14), 1e-5f);

        assertEquals(0.5f / sqrt(2) / 2, tvec.get(15), 1e-5f);

        assertEquals(0.1f / sqrt(2) / 2, tvec.get(16), 1e-5f);
        assertEquals(0.3f / sqrt(2) / 2, tvec.get(17), 1e-5f);
        assertEquals(0.5f / sqrt(2) / 2, tvec.get(18), 1e-5f);

        // 0 land 3 snow
        agg.aggregateTemporal(ctx, vec(0f, 1f, 3f, 3f, 2f,
                                       0f, 1f, 2f, 1f, 1f,
                                       1.1f, 1.3f, 1.5f,
                                       0.5f,
                                       0.1f, 0.3f, 0.5f), 9, tvec);
        assertEquals(2f / 9f, tvec.get(0), 0.0f);
        assertEquals(2f / 9f, tvec.get(1), 0.0f);
        assertEquals(4f / 9f, tvec.get(2), 0.0f);
        assertEquals(6f / 9f, tvec.get(3), 0.0f);
        assertEquals(4f / 9f, tvec.get(4), 0.0f);

        assertEquals(1f, tvec.get(5), 0.0f);
        assertEquals(2f, tvec.get(6), 0.0f);
        assertEquals(3f, tvec.get(7), 0.0f);
        assertEquals(2f, tvec.get(8), 0.0f);
        assertEquals(2f, tvec.get(9), 0.0f);

        assertEquals(1.0f, tvec.get(10), 0.0f);
        assertEquals(1f / sqrt(2), tvec.get(11), 1e-5f);

        assertEquals(1.1f / sqrt(2) / 2, tvec.get(12), 1e-5f);
        assertEquals(1.3f / sqrt(2) / 2, tvec.get(13), 1e-5f);
        assertEquals(1.5f / sqrt(2) / 2, tvec.get(14), 1e-5f);

        assertEquals(0.5f / sqrt(2) / 2, tvec.get(15), 1e-5f);

        assertEquals(0.1f / sqrt(2) / 2, tvec.get(16), 1e-5f);
        assertEquals(0.3f / sqrt(2) / 2, tvec.get(17), 1e-5f);
        assertEquals(0.5f / sqrt(2) / 2, tvec.get(18), 1e-5f);

        // 3 land 0 snow
        agg.aggregateTemporal(ctx, vec(3f, 0f, 0f, 0f, 0f,
                                       2f, 0f, 0f, 0f, 0f,
                                       1.1f, 1.3f, 1.5f,
                                       0.5f,
                                       0.1f, 0.3f, 0.5f), 3, tvec);
        assertEquals(2f / 9f + 3f / 3f, tvec.get(0), 0.0f);
        assertEquals(2f / 9f, tvec.get(1), 0.0f);
        assertEquals(4f / 9f, tvec.get(2), 0.0f);
        assertEquals(6f / 9f, tvec.get(3), 0.0f);
        assertEquals(4f / 9f, tvec.get(4), 0.0f);

        assertEquals(3.0f, tvec.get(5), 0.0f);
        assertEquals(2.0f, tvec.get(6), 0.0f);
        assertEquals(3.0f, tvec.get(7), 0.0f);
        assertEquals(2.0f, tvec.get(8), 0.0f);
        assertEquals(2.0f, tvec.get(9), 0.0f);

        assertEquals(1.0f, tvec.get(10), 0.0f);
        assertEquals(1f / sqrt(2) + 1f / sqrt(3), tvec.get(11), 1e-5f);

        assertEquals(1.1f / sqrt(2) / 2 + 1.1f / sqrt(3) / 3, tvec.get(12), 1e-5f);
        assertEquals(1.3f / sqrt(2) / 2 + 1.3f / sqrt(3) / 3, tvec.get(13), 1e-5f);
        assertEquals(1.5f / sqrt(2) / 2 + 1.5f / sqrt(3) / 3, tvec.get(14), 1e-5f);

        assertEquals(0.5f / sqrt(2) / 2 + 0.5f / sqrt(3) / 3, tvec.get(15), 1e-5f);

        assertEquals(0.1f / sqrt(2) / 2 + 0.1f / sqrt(3) / 3, tvec.get(16), 1e-5f);
        assertEquals(0.3f / sqrt(2) / 2 + 0.3f / sqrt(3) / 3, tvec.get(17), 1e-5f);
        assertEquals(0.5f / sqrt(2) / 2 + 0.5f / sqrt(3) / 3, tvec.get(18), 1e-5f);
    }

    @Test
    public void testTemporalBinning_startwWithSnow() {
        VectorImpl tvec = vec(19, NaN);
        agg.initTemporal(ctx, tvec);
        assertEquals(0.0f, tvec.get(0), 0.0f);
        assertEquals(0.0f, tvec.get(1), 0.0f);
        assertEquals(0.0f, tvec.get(2), 0.0f);
        assertEquals(0.0f, tvec.get(3), 0.0f);
        assertEquals(0.0f, tvec.get(4), 0.0f);

        assertEquals(0.0f, tvec.get(5), 0.0f);
        assertEquals(0.0f, tvec.get(6), 0.0f);
        assertEquals(0.0f, tvec.get(7), 0.0f);
        assertEquals(0.0f, tvec.get(8), 0.0f);
        assertEquals(0.0f, tvec.get(9), 0.0f);

        assertEquals(0.0f, tvec.get(10), 0.0f);
        assertEquals(0.0f, tvec.get(11), 0.0f);

        assertEquals(0.0f, tvec.get(12), 0.0f);
        assertEquals(0.0f, tvec.get(13), 0.0f);
        assertEquals(0.0f, tvec.get(14), 0.0f);

        assertEquals(0.0f, tvec.get(15), 0.0f);

        assertEquals(0.0f, tvec.get(16), 0.0f);
        assertEquals(0.0f, tvec.get(17), 0.0f);
        assertEquals(0.0f, tvec.get(18), 0.0f);

        // 0 land 1 snow
        agg.aggregateTemporal(ctx, vec(0f, 1f, 1f, 3f, 2f,
                                       0f, 1f, 1f, 1f, 1f,
                                       1.1f, 1.3f, 1.5f,
                                       0.5f,
                                       0.1f, 0.3f, 0.5f), 7, tvec);
        assertEquals(0f / 7f, tvec.get(0), 0.0f);
        assertEquals(1f / 7f, tvec.get(1), 0.0f);
        assertEquals(1f / 7f, tvec.get(2), 0.0f);
        assertEquals(3f / 7f, tvec.get(3), 0.0f);
        assertEquals(2f / 7f, tvec.get(4), 0.0f);

        assertEquals(0.0f, tvec.get(5), 0.0f);
        assertEquals(1.0f, tvec.get(6), 0.0f);
        assertEquals(1.0f, tvec.get(7), 0.0f);
        assertEquals(1.0f, tvec.get(8), 0.0f);
        assertEquals(1.0f, tvec.get(9), 0.0f);

        assertEquals(3.0f, tvec.get(10), 0.0f);
        assertEquals(1f / sqrt(1), tvec.get(11), 1e-5f);

        assertEquals(1.1f / sqrt(1) / 1, tvec.get(12), 1e-5f);
        assertEquals(1.3f / sqrt(1) / 1, tvec.get(13), 1e-5f);
        assertEquals(1.5f / sqrt(1) / 1, tvec.get(14), 1e-5f);

        assertEquals(0.5f / sqrt(1) / 1, tvec.get(15), 1e-5f);

        assertEquals(0.1f / sqrt(1) / 1, tvec.get(16), 1e-5f);
        assertEquals(0.3f / sqrt(1) / 1, tvec.get(17), 1e-5f);
        assertEquals(0.5f / sqrt(1) / 1, tvec.get(18), 1e-5f);

        // 0 land 3 snow
        agg.aggregateTemporal(ctx, vec(0f, 1f, 3f, 3f, 2f,
                                       0f, 1f, 1f, 1f, 1f,
                                       1.1f, 1.3f, 1.5f,
                                       0.5f,
                                       0.1f, 0.3f, 0.5f), 9, tvec);
        assertEquals(0f / 7f + 0f/9f, tvec.get(0), 0.0f);
        assertEquals(1f / 7f + 1f/9f, tvec.get(1), 0.0f);
        assertEquals(1f / 7f + 3f/9f, tvec.get(2), 0.0f);
        assertEquals(3f / 7f + 3f/9f, tvec.get(3), 0.0f);
        assertEquals(2f / 7f + 2f / 9f, tvec.get(4), 0.0f);

        assertEquals(0f, tvec.get(5), 0.0f);
        assertEquals(2f, tvec.get(6), 0.0f);
        assertEquals(2f, tvec.get(7), 0.0f);
        assertEquals(2f, tvec.get(8), 0.0f);
        assertEquals(2f, tvec.get(9), 0.0f);

        assertEquals(3.0f, tvec.get(10), 0.0f);
        assertEquals(1f / sqrt(1) + 1f / sqrt(3), tvec.get(11), 1e-5f);

        assertEquals(1.1f / sqrt(1) / 1 + 1.1f / sqrt(3) / 3, tvec.get(12), 1e-5f);
        assertEquals(1.3f / sqrt(1) / 1 + 1.3f / sqrt(3) / 3, tvec.get(13), 1e-5f);
        assertEquals(1.5f / sqrt(1) / 1 + 1.5f / sqrt(3) / 3, tvec.get(14), 1e-5f);

        assertEquals(0.5f / sqrt(1) / 1 + 0.5f / sqrt(3) / 3, tvec.get(15), 1e-5f);

        assertEquals(0.1f / sqrt(1) / 1 + 0.1f / sqrt(3) / 3, tvec.get(16), 1e-5f);
        assertEquals(0.3f / sqrt(1) / 1 + 0.3f / sqrt(3) / 3, tvec.get(17), 1e-5f);
        assertEquals(0.5f / sqrt(1) / 1 + 0.5f / sqrt(3) / 3, tvec.get(18), 1e-5f);


        // 1 land 0 snow
        agg.aggregateTemporal(ctx, vec(1f, 0f, 0f, 0f, 0f,
                                       1f, 0f, 0f, 0f, 0f,
                                       1.1f, 1.3f, 1.5f,
                                       0.5f,
                                       0.1f, 0.3f, 0.5f), 1, tvec);

        assertEquals(0f / 7f + 0f/9f + 1, tvec.get(0), 0.0f);
        assertEquals(1f / 7f + 1f/9f, tvec.get(1), 0.0f);
        assertEquals(1f / 7f + 3f/9f, tvec.get(2), 0.0f);
        assertEquals(3f / 7f + 3f/9f, tvec.get(3), 0.0f);
        assertEquals(2f / 7f + 2f/9f, tvec.get(4), 0.0f);

        assertEquals(1f, tvec.get(5), 0.0f);
        assertEquals(2f, tvec.get(6), 0.0f);
        assertEquals(2f, tvec.get(7), 0.0f);
        assertEquals(2f, tvec.get(8), 0.0f);
        assertEquals(2f, tvec.get(9), 0.0f);

        assertEquals(1.0f, tvec.get(10), 0.0f);
        assertEquals(1f / sqrt(1), tvec.get(11), 1e-5f);

        assertEquals(1.1f / sqrt(1) / 1, tvec.get(12), 1e-5f);
        assertEquals(1.3f / sqrt(1) / 1, tvec.get(13), 1e-5f);
        assertEquals(1.5f / sqrt(1) / 1, tvec.get(14), 1e-5f);

        assertEquals(0.5f / sqrt(1) / 1, tvec.get(15), 1e-5f);

        assertEquals(0.1f / sqrt(1) / 1, tvec.get(16), 1e-5f);
        assertEquals(0.3f / sqrt(1) / 1, tvec.get(17), 1e-5f);
        assertEquals(0.5f / sqrt(1) / 1, tvec.get(18), 1e-5f);
    }

    @Test
    public void testComputeOutput() throws Exception {
        VectorImpl ovec = vec(18, NaN);
        agg.computeOutput(vec(1f, 0f, 3f, 1f, 1f,
                              1f, 0f, 2f, 1f, 1f,
                              1f, 1f,
                              1.1f, 1.3f, 1.5f,
                              0.5f,
                              0.1f, 0.3f, 0.5f), ovec);

        assertEquals(1.0f, ovec.get(0), 0.0f);
        assertEquals(0.0f, ovec.get(1), 0.0f);
        assertEquals(3.0f, ovec.get(2), 0.0f);
        assertEquals(1.0f, ovec.get(3), 0.0f);
        assertEquals(1.0f, ovec.get(4), 0.0f);

        assertEquals(1.0f, ovec.get(5), 0.0f);
        assertEquals(0.0f, ovec.get(6), 0.0f);
        assertEquals(2.0f, ovec.get(7), 0.0f);
        assertEquals(1.0f, ovec.get(8), 0.0f);
        assertEquals(1.0f, ovec.get(9), 0.0f);

        assertEquals(1.0f, ovec.get(10), 0.0f);

        assertEquals(1.1f / 1f, ovec.get(11), 1e-5f);
        assertEquals(1.3f / 1f, ovec.get(12), 1e-5f);
        assertEquals(1.5f / 1f, ovec.get(13), 1e-5f);

        assertEquals(0.5f / 1f, ovec.get(14), 1e-5f);

        assertEquals(0.1f / 1f, ovec.get(15), 1e-5f);
        assertEquals(0.3f / 1f, ovec.get(16), 1e-5f);
        assertEquals(0.5f / 1f, ovec.get(17), 1e-5f);
    }

    @Test
    public void testCalculateStatus() throws Exception {
        assertEquals(1, AggregatorSR.calculateStatus(1, 0, 2, 2, 2));
        assertEquals(3, AggregatorSR.calculateStatus(0, 0, 2, 2, 2));
        assertEquals(3, AggregatorSR.calculateStatus(0, 1, 2, 2, 2));
        assertEquals(2, AggregatorSR.calculateStatus(0, 3, 2, 2, 2));
        assertEquals(4, AggregatorSR.calculateStatus(0, 0, 0, 7, 2));
        assertEquals(6, AggregatorSR.calculateStatus(0, 0, 0, 0, 0));
    }

    @Test
    public void testPixelCounter() throws Exception {
        AggregatorSR.PixelCounter pc = new AggregatorSR.PixelCounter(1);
        assertEquals(0, pc.getDifferentPoints(0));
        pc.addPoint(0, 1, 2);
        assertEquals(1, pc.getDifferentPoints(0));
        pc.addPoint(0, 1, 2);
        assertEquals(1, pc.getDifferentPoints(0));
        pc.addPoint(0, 2, 1);
        assertEquals(2, pc.getDifferentPoints(0));
        pc.addPoint(0, 3, 4);
        pc.addPoint(0, 5, 6);
        assertEquals(4, pc.getDifferentPoints(0));
        pc.addPoint(0, 2, 1);
        pc.addPoint(0, 3, 4);
        pc.addPoint(0, 5, 6);
        assertEquals(4, pc.getDifferentPoints(0));
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
