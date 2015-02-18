/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.l2tol3;

import org.esa.beam.binning.Observation;
import org.esa.beam.binning.support.ObservationImpl;
import org.esa.beam.binning.support.VariableContextImpl;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class RatioCalculatorTest {

    @Test
    public void testCalculateRatio_Simple() throws Exception {
        VariableContextImpl l2VariableContext = new VariableContextImpl();
        l2VariableContext.defineVariable("xaxis");
        l2VariableContext.defineVariable("radiance_1");

        String[] l3FeatureNames = new String[]{"radiance_1_mean"};

        final long l3BinIndex = 42L;
        float[] meanL3Values = {5f};
        Map<Long, float[]> l3MeanValueMap = new HashMap<>();
        l3MeanValueMap.put(l3BinIndex, meanL3Values);
        RatioCalculator ratioCalculator = new RatioCalculator(l2VariableContext, l3FeatureNames, l3MeanValueMap);

        Observation l2Observation = new ObservationImpl(0, 0, 0, 10f, 20f);

        Observation observation = ratioCalculator.calculateRatio(l3BinIndex, l2Observation);
        assertNotNull(observation);
        assertEquals(l2Observation.size(), observation.size());
        assertEquals(10f, observation.get(0), 1e-5); // xaxis
        assertEquals(20f / 5f, observation.get(1), 1e-5); // radiance_1 / radiance_1_mean
    }

    @Test
    public void testCalculateRatio_complex() throws Exception {
        VariableContextImpl l2VariableContext = new VariableContextImpl();
        l2VariableContext.defineVariable("xaxis");
        l2VariableContext.defineVariable("radiance_1");
        l2VariableContext.defineVariable("radiance_2");
        l2VariableContext.defineVariable("radiance_3");

        String[] l3FeatureNames = new String[]{
                "radiance_1_mean", "radiance_1_sigma",
                "radiance_2_mean", "radiance_2_sigma",
                "radiance_3_mean", "radiance_3_sigma"};

        final long l3BinIndex = 42L;
        float[] meanL3Values = {2f, 3f, 5f, 3, 10f, 3f};
        Map<Long, float[]> l3MeanValueMap = new HashMap<>();
        l3MeanValueMap.put(l3BinIndex, meanL3Values);

        RatioCalculator ratioCalculator = new RatioCalculator(l2VariableContext, l3FeatureNames, l3MeanValueMap);

        Observation l2Observation = new ObservationImpl(0, 0, 0, 10f, 20f, 30f, 40f);

        Observation observation = ratioCalculator.calculateRatio(l3BinIndex, l2Observation);
        assertNotNull(observation);
        assertEquals(l2Observation.size(), observation.size());
        assertEquals(10f, observation.get(0), 1e-5); // xaxis
        assertEquals(20f / 2f, observation.get(1), 1e-5); // radiance_1 / radiance_1_mean
        assertEquals(30f / 5f, observation.get(2), 1e-5); // radiance_2 / radiance_2_mean
        assertEquals(40f / 10f, observation.get(3), 1e-5); // radiance_3 / radiance_3_mean
    }

    @Test
    public void testIsRatioVariable() throws Exception {
        VariableContextImpl l2VariableContext = new VariableContextImpl();
        l2VariableContext.defineVariable("xaxis");
        l2VariableContext.defineVariable("radiance_1");
        l2VariableContext.defineVariable("radiance_2");

        String[] l3FeatureNames = new String[]{"radiance_1_mean", "radiance_1_sigma", "radiance_2_mean"};
        boolean[] ratioVariables = RatioCalculator.getRatioVariables(l2VariableContext, l3FeatureNames);
        assertEquals(3, ratioVariables.length);
        assertEquals(false, ratioVariables[0]);
        assertEquals(true, ratioVariables[1]);
        assertEquals(true, ratioVariables[2]);
    }

}