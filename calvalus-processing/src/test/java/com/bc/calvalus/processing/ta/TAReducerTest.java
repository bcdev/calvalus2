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

package com.bc.calvalus.processing.ta;


import com.bc.ceres.binding.PropertySet;
import org.esa.beam.binning.TemporalBin;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3TemporalBin;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.binning.AggregatorConfig;
import org.esa.beam.binning.aggregators.AggregatorAverageML;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.sqrt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TAReducerTest {

    private TAReducer taReducer;

    @Before
    public void setUp() throws Exception {
        Configuration configuration = createConfiguration();
        taReducer = new TAReducer();
        taReducer.setConf(configuration);
    }
    // AggregatorAverageML
    // createFeatureNames(varName, "sum_x", "sum_xx"),
    // createFeatureNames(varName, "sum_x", "sum_xx", "sum_w"),
    // createFeatureNames(varName, "mean", "sigma", "median", "mode"),

    @Test
    public void testReduce() throws Exception {
        List<L3TemporalBin> temporalBins = new ArrayList<L3TemporalBin>();
        temporalBins.add(createTBin(1, 10, 0.2f, 0.15f, 0.3f));
        temporalBins.add(createTBin(2, 7, 0.4f, 0.25f, 0.3f));
        temporalBins.add(createTBin(4, 6, 0.6f, 0.35f, 0.3f));

        TAPoint taPoint = taReducer.computeTaPoint("northsea", temporalBins);

        assertNotNull(taPoint);
        assertEquals("northsea", taPoint.getRegionName());
        assertEquals("2010-01-01", taPoint.getStartDate());
        assertEquals("2010-01-10", taPoint.getStopDate());

        TemporalBin temporalBin = taPoint.getTemporalBin();
        assertNotNull(temporalBin);
        assertEquals(1 + 2 + 4, temporalBin.getNumPasses());
        assertEquals(10 + 7 + 6, temporalBin.getNumObs());

        float[] featureValues = temporalBin.getFeatureValues();
        assertEquals(3, featureValues.length);
        assertEquals(0.2f + 0.4f + 0.6f, featureValues[0], 1e-5f);
        assertEquals(0.15f + 0.25f + 0.35f, featureValues[1], 1e-5f);
        assertEquals(sqrt(10) + sqrt(7) + sqrt(6), featureValues[2], 1e-5f);

        assertEquals("TAPoint{regionName=northsea, startDate=2010-01-01, stopDate=2010-01-10, temporalBin=L3TemporalBin{index=-1, numObs=23, numPasses=7, featureValues=[1.2, 0.75, 8.257519]}}", taPoint.toString());
    }

    private L3TemporalBin createTBin(int numPasses, int numObs, float... values) {
        L3TemporalBin tBin = new L3TemporalBin(-1, values.length);
        tBin.setNumPasses(numPasses);
        tBin.setNumObs(numObs);
        System.arraycopy(values, 0, tBin.getFeatureValues(), 0, values.length);
        return tBin;
    }

    private Configuration createConfiguration() {
        Configuration configuration;
        configuration = new Configuration();
        L3Config l3Config = createL3Config();
        configuration.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3Config.toXml());
        configuration.set(JobConfigNames.CALVALUS_MIN_DATE, "2010-01-01");
        configuration.set(JobConfigNames.CALVALUS_MAX_DATE, "2010-01-10");
        return configuration;
    }

    private L3Config createL3Config() {
        L3Config l3Config = new L3Config();
        l3Config.setNumRows(2160);
        l3Config.setSuperSampling(1); // unused
        l3Config.setMaskExpr(""); // unused
        AggregatorConfig aggConf = new AggregatorAverageML.Descriptor().createConfig();
        PropertySet aggProperties = aggConf.asPropertySet();
        aggProperties.setValue("varName", "chl_conc");
        aggProperties.setValue("weightCoeff", 0.5);
        aggProperties.setValue("fillValue", Float.NaN);
//        aggConf.setVarName("chl_conc");
//        aggConf.setWeightCoeff(0.5);
//        aggConf.setFillValue(Float.NaN);
        l3Config.setAggregatorConfigs(aggConf);
        return l3Config;
    }
}
