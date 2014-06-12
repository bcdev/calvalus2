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

import com.bc.ceres.binding.Property;
import com.bc.ceres.binding.PropertySet;
import org.esa.beam.binning.BinManager;
import org.esa.beam.binning.CellProcessorConfig;
import org.esa.beam.binning.CompositingType;
import org.esa.beam.binning.PlanetaryGrid;
import org.esa.beam.binning.VariableContext;
import org.esa.beam.binning.aggregators.AggregatorAverage;
import org.esa.beam.binning.aggregators.AggregatorMinMax;
import org.esa.beam.binning.aggregators.AggregatorOnMaxSet;
import org.esa.beam.binning.cellprocessor.FeatureSelection;
import org.esa.beam.binning.operator.BinningConfig;
import org.esa.beam.binning.support.PlateCarreeGrid;
import org.esa.beam.binning.support.SEAGrid;
import org.esa.beam.util.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import static org.junit.Assert.*;

public class BinningConfigTest {

    private BinningConfig binningConfig;

    @Before
    public void createL3Config() throws Exception {
        binningConfig = loadConfig("l3-parameters.xml");
    }

    @Test
    public void testPlanetaryGrid() {
        assertEquals(PlateCarreeGrid.class.getName(), binningConfig.getPlanetaryGrid());
    }

    @Test
    public void testPlanetaryGridCreation() {
        PlanetaryGrid grid = new BinningConfig().createPlanetaryGrid();
        assertEquals(2160, grid.getNumRows());
        assertEquals(SEAGrid.class, grid.getClass());

        grid = binningConfig.createPlanetaryGrid();
        assertEquals(4320, grid.getNumRows());
        assertEquals(PlateCarreeGrid.class, grid.getClass());
    }

    @Test
    public void testVariableContext() {
        VariableContext varCtx = binningConfig.createVariableContext();

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

        assertEquals("!l2_flags.INVALID && l2_flags.WATER", varCtx.getValidMaskExpression());

        assertEquals("ndvi", varCtx.getVariableName(0));
        assertEquals("(reflec_10 - reflec_6) / (reflec_10 + reflec_6)", varCtx.getVariableExpression(0));

        assertEquals("algal2", varCtx.getVariableName(3));
        assertEquals(null, varCtx.getVariableExpression(3));

        assertEquals("reflec_7", varCtx.getVariableName(6));
        assertEquals(null, varCtx.getVariableExpression(6));

    }

    @Test
    public void testBinManager() {
        BinManager binManager = binningConfig.createBinningContext(null, null, null).getBinManager();
        assertEquals(6, binManager.getAggregatorCount());
        assertEquals(AggregatorAverage.class, binManager.getAggregator(0).getClass());
        assertEquals(AggregatorAverage.class, binManager.getAggregator(1).getClass());
        assertEquals(AggregatorAverage.class, binManager.getAggregator(2).getClass());
        assertEquals(AggregatorAverage.class, binManager.getAggregator(3).getClass());
        assertEquals(AggregatorOnMaxSet.class, binManager.getAggregator(4).getClass());
        assertEquals(AggregatorMinMax.class, binManager.getAggregator(5).getClass());
    }

    @Test
    public void testNumRows() {
        assertEquals(4320, binningConfig.getNumRows());
    }

    @Test
    public void testCompositingType() {
        assertEquals(CompositingType.MOSAICKING, binningConfig.getCompositingType());
    }

    @Test
    public void testL3configForCellProcressing() throws Exception {
        BinningConfig binningConfig = loadConfig("l3-cellProcessing.xml");
        assertNotNull(binningConfig);
        CellProcessorConfig postProcessorConfig = binningConfig.getPostProcessorConfig();
        assertNotNull(postProcessorConfig);
        assertSame(FeatureSelection.Config.class, postProcessorConfig.getClass());
        PropertySet propertySet = postProcessorConfig.asPropertySet();
        assertNotNull(propertySet);
        Property[] properties = propertySet.getProperties();
        assertEquals(2, properties.length);
        System.out.println("properties = " + Arrays.toString(properties));
        assertEquals("Selection", propertySet.getProperty("type").getValue());
        String[] expected = {"tsm_mean", " tsm_sigma", " chl_min", "cmax = chl_max"};
        String[]  actual = propertySet.getProperty("varNames").getValue();
        assertArrayEquals(expected, actual);
    }

    private BinningConfig loadConfig(String configPath) throws Exception {
        return BinningConfig.fromXml(loadConfigProperties(configPath));
    }

    private String loadConfigProperties(String configPath) throws IOException {
        InputStreamReader inputStreamReader = new InputStreamReader(getClass().getResourceAsStream(configPath));
        try {
            return FileUtils.readText(inputStreamReader).trim();
        } finally {
            inputStreamReader.close();
        }
    }

}
