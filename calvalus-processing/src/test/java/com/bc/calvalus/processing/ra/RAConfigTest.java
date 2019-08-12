/*
 * Copyright (C) 2017 Brockmann Consult GmbH (info@brockmann-consult.de) 
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

package com.bc.calvalus.processing.ra;

import org.junit.Test;

import static org.junit.Assert.*;

public class RAConfigTest {

    @Test
    public void textXML() throws Exception {
        RAConfig raConfig = new RAConfig();
        raConfig.setRegionSource("/calvalus/home/marcoz/shapefiles/HELCOM_grid100_LAEA5210.zip");
        raConfig.setRegionSourceAttributeName("CellCode");
        raConfig.setRegionSourceAttributeFilter("40N2, 40N3");
        raConfig.setGoodPixelExpression("not cloud and water");
        raConfig.setPercentiles(2,50,99);
        raConfig.setBandConfigs(new RAConfig.BandConfig("chl"), new RAConfig.BandConfig("refl_1", 20, 0.0, 1.0));
        String xml = raConfig.toXml();
        String expected = "<parameters>\n" +
                "  <regionSource>/calvalus/home/marcoz/shapefiles/HELCOM_grid100_LAEA5210.zip</regionSource>\n" +
                "  <regionSourceAttributeName>CellCode</regionSourceAttributeName>\n" +
                "  <regionSourceAttributeFilter>40N2, 40N3</regionSourceAttributeFilter>\n" +
                "  <withRegionEnvelope>false</withRegionEnvelope>\n" +
                "  <withProductNames>false</withProductNames>\n" +
                "  <goodPixelExpression>not cloud and water</goodPixelExpression>\n" +
                "  <percentiles>2,50,99</percentiles>\n" +
                "  <bands>\n" +
                "    <band>\n" +
                "      <name>chl</name>\n" +
                "    </band>\n" +
                "    <band>\n" +
                "      <name>refl_1</name>\n" +
                "      <numBins>20</numBins>\n" +
                "      <min>0.0</min>\n" +
                "      <max>1.0</max>\n" +
                "    </band>\n" +
                "  </bands>\n" +
                "  <writePerRegion>true</writePerRegion>\n" +
                "  <writeSeparateHistogram>true</writeSeparateHistogram>\n" +
                "  <writePixelValues>false</writePixelValues>\n" +
                "  <binValuesAsRatio>false</binValuesAsRatio>\n" +
                "</parameters>";
        
        assertEquals(expected, xml);
    }

    @Test
    public void parseXmlTest() throws Exception {
        String expected = "<parameters>\n" +
                "  <regionSource>/calvalus/home/marcoz/shapefiles/HELCOM_grid100_LAEA5210.zip</regionSource>\n" +
                "  <regionSourceAttributeName>CellCode</regionSourceAttributeName>\n" +
                "  <regionSourceAttributeFilter>40N2, 40N3</regionSourceAttributeFilter>\n" +
                "  <withRegionEnvelope>false</withRegionEnvelope>\n" +
                "  <withProductNames>false</withProductNames>\n" +
                "  <goodPixelExpression>not cloud and water</goodPixelExpression>\n" +
                "  <!-- <percentiles>2,50,99</percentiles> -->\n" +
                "  <percentiles></percentiles>\n" +
                "  <bands>\n" +
                "    <band>\n" +
                "      <name>chl</name>\n" +
                "    </band>\n" +
                "    <band>\n" +
                "      <name>refl_1</name>\n" +
                "      <numBins>20</numBins>\n" +
                "      <min>0.0</min>\n" +
                "      <max>1.0</max>\n" +
                "    </band>\n" +
                "  </bands>\n" +
                "  <writePerRegion>true</writePerRegion>\n" +
                "  <writeSeparateHistogram>true</writeSeparateHistogram>\n" +
                "  <writePixelValues>false</writePixelValues>\n" +
                "</parameters>";
        RAConfig raConfig = RAConfig.fromXml(expected);
        assertEquals(false, raConfig.isBinValuesAsRatio());
        assertNull(raConfig.getPercentiles());
    }
}