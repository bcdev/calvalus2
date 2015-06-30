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


import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.l3.L3TemporalBin;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.binding.PropertySet;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.binning.AggregatorConfig;
import org.esa.snap.binning.TemporalBin;
import org.esa.snap.binning.aggregators.AggregatorAverage;
import org.esa.snap.binning.operator.BinningConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

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

        L3TemporalBin outputBin = (L3TemporalBin) taReducer.binManager.createTemporalBin(-1);
        for (L3TemporalBin bin : temporalBins) {
            taReducer.binManager.aggregateTemporalBin(bin, outputBin);
        }
        TAPoint taPoint = new TAPoint("northsea", taReducer.minDate, taReducer.maxDate, outputBin);

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
        assertEquals(8.4f, featureValues[0], 1e-5f);
        assertEquals(5.35f, featureValues[1], 1e-5f);
        assertEquals(23f, featureValues[2], 1e-5f);

        assertEquals("TAPoint{regionName=northsea, startDate=2010-01-01, stopDate=2010-01-10, temporalBin=L3TemporalBin{index=-1, numObs=23, numPasses=7, featureValues=[8.400001, 5.35, 23.0]}}", taPoint.toString());
    }

    private L3TemporalBin createTBin(int numPasses, int numObs, float... values) {
        L3TemporalBin tBin = new L3TemporalBin(-1, values.length);
        tBin.setNumPasses(numPasses);
        tBin.setNumObs(numObs);
        System.arraycopy(values, 0, tBin.getFeatureValues(), 0, values.length);
        return tBin;
    }

    private Configuration createConfiguration() throws BindingException {
        Configuration configuration;
        configuration = new Configuration();
        BinningConfig binningConfig = createL3Config();
        configuration.set(JobConfigNames.CALVALUS_L3_PARAMETERS, binningConfig.toXml());
        configuration.set(JobConfigNames.CALVALUS_MIN_DATE, "2010-01-01");
        configuration.set(JobConfigNames.CALVALUS_MAX_DATE, "2010-01-10");
        TAConfig taConfig = TAConfig.fromXml("<parameters><regions><region><name>aoi</name><geometry>polygon((0 0,0 1,1 1,1 0,0 0))</geometry></region></regions></parameters>");
        configuration.set(JobConfigNames.CALVALUS_TA_PARAMETERS, taConfig.toXml());
        return configuration;
    }

    private BinningConfig createL3Config() {
        BinningConfig binningConfig = new BinningConfig();
        binningConfig.setNumRows(2160);
        binningConfig.setSuperSampling(1); // unused
        binningConfig.setMaskExpr(""); // unused
        AggregatorConfig aggConf = new AggregatorAverage.Descriptor().createConfig();
        PropertySet aggProperties = aggConf.asPropertySet();
        aggProperties.setValue("varName", "chl_conc");
        aggProperties.setValue("weightCoeff", 1.0);
        binningConfig.setAggregatorConfigs(aggConf);
        return binningConfig;
    }
}
