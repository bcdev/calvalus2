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

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.binning.BinningGrid;
import com.bc.calvalus.binning.IsinBinningGrid;
import com.bc.calvalus.binning.VariableContext;
import com.bc.calvalus.binning.aggregators.AggregatorAverage;
import com.bc.calvalus.binning.aggregators.AggregatorAverageML;
import com.bc.calvalus.binning.aggregators.AggregatorMinMax;
import com.bc.calvalus.binning.aggregators.AggregatorOnMaxSet;
import com.bc.calvalus.processing.WpsConfig;
import com.bc.ceres.binding.BindingException;
import org.esa.beam.util.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;

import static org.junit.Assert.*;

public class L3ConfigTest {

    private L3Config l3Config;

    @Before
    public void createL3Config() throws IOException, SAXException, ParserConfigurationException, BindingException {
        l3Config = loadConfig("l3-request.xml");
    }

    @Test
    public void testBinningGrid() {
        BinningGrid grid = new L3Config().createBinningGrid();
        assertEquals(2160, grid.getNumRows());
        assertEquals(IsinBinningGrid.class, grid.getClass());

        grid = l3Config.createBinningGrid();
        assertEquals(4320, grid.getNumRows());
        assertEquals(IsinBinningGrid.class, grid.getClass());

    }

//    @Test
//    public void testStartEndTime() throws ParseException {
//        Properties properties = new Properties();
//        com.bc.calvalus.binning.job.BeamL3Config l3Config = new com.bc.calvalus.binning.job.BeamL3Config(properties);
//        properties.setProperty(CONFNAME_L3_START_DATE, "2008-06-01");
//        ProductData.UTC startTime = l3Config.getStartTime();
//        assertNotNull(startTime);
//        Calendar expectedStart = ProductData.UTC.parse("01-JUN-2008 00:00:00").getAsCalendar();
//        assertEqualsCalendar(expectedStart, startTime.getAsCalendar());
//
//        ProductData.UTC endTime = l3Config.getEndTime();
//        assertNotNull(endTime);
//        Calendar expectedEnd = ProductData.UTC.parse("16-JUN-2008 00:00:00").getAsCalendar();
//        assertEqualsCalendar(expectedEnd, endTime.getAsCalendar());
//    }

    private static void assertEqualsCalendar(Calendar expected, Calendar actual) {
        assertEquals(expected.get(Calendar.YEAR), actual.get(Calendar.YEAR));
        assertEquals(expected.get(Calendar.MONTH), actual.get(Calendar.MONTH));
        assertEquals(expected.get(Calendar.DAY_OF_MONTH), actual.get(Calendar.DAY_OF_MONTH));
    }

    @Test
    public void testVariableContext() {
        VariableContext varCtx = l3Config.createVariableContext();

        assertEquals(8, varCtx.getVariableCount());

        assertEquals(0, varCtx.getVariableIndex("ndvi"));
        assertEquals(1, varCtx.getVariableIndex("tsm"));
        assertEquals(2, varCtx.getVariableIndex("algal1"));
        assertEquals(3, varCtx.getVariableIndex("algal2"));
        assertEquals(4, varCtx.getVariableIndex("chl"));
        assertEquals(5, varCtx.getVariableIndex("reflec_3"));
        assertEquals(6, varCtx.getVariableIndex("reflec_7"));
        assertEquals(7, varCtx.getVariableIndex("reflec_8"));
        assertEquals(-1, varCtx.getVariableIndex("reflec_6"));
        assertEquals(-1, varCtx.getVariableIndex("reflec_10"));

        assertEquals("!l2_flags.INVALID && l2_flags.WATER", varCtx.getMaskExpr());

        assertEquals("ndvi", varCtx.getVariableName(0));
        assertEquals("(reflec_10 - reflec_6) / (reflec_10 + reflec_6)", varCtx.getVariableExpr(0));

        assertEquals("algal2", varCtx.getVariableName(3));
        assertEquals(null, varCtx.getVariableExpr(3));

        assertEquals("reflec_7", varCtx.getVariableName(6));
        assertEquals(null, varCtx.getVariableExpr(6));

    }

    @Test
    public void testBinManager() {
        BinManager binManager = l3Config.createBinningContext().getBinManager();
        assertEquals(6, binManager.getAggregatorCount());
        assertEquals(AggregatorAverage.class, binManager.getAggregator(0).getClass());
        assertEquals(AggregatorAverageML.class, binManager.getAggregator(1).getClass());
        assertEquals(AggregatorAverageML.class, binManager.getAggregator(2).getClass());
        assertEquals(AggregatorAverageML.class, binManager.getAggregator(3).getClass());
        assertEquals(AggregatorOnMaxSet.class, binManager.getAggregator(4).getClass());
        assertEquals(AggregatorMinMax.class, binManager.getAggregator(5).getClass());
    }

    @Test
    public void testNumRows() {
        assertEquals(4320, l3Config.getNumRows());
    }

    private L3Config loadConfig(String configPath) throws IOException, SAXException, ParserConfigurationException, BindingException {
        String wpsRequest = loadConfigProperties(configPath);
        WpsConfig wpsConfig = new WpsConfig(wpsRequest);
        return L3Config.fromXml(wpsConfig.getLevel3Parameters());
    }

    private String loadConfigProperties(String configPath) throws IOException {
        InputStreamReader inputStreamReader = new InputStreamReader(getClass().getResourceAsStream(configPath));
        return FileUtils.readText(inputStreamReader).trim();
    }

}
