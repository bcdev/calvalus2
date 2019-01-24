/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.executable;


import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

public class PropertiesHandlerTest {

    @Test
    public void testAsProperties_KeyValue() throws Exception {
        Properties properties = PropertiesHandler.asProperties("key = value1\n  key2   = value3");
        assertNotNull(properties);
        assertEquals(2, properties.size());

        assertEquals("value1", properties.getProperty("key"));
        assertEquals("value3", properties.getProperty("key2"));

    }

    @Test
    public void testAsProperties_XML_oneProperty() throws Exception {
        Properties properties = PropertiesHandler.asProperties(
                "<parameters>\n" +
                        "  <key>value1</key>\n" +
                        "</parameters>"
        );
        assertNotNull(properties);
        assertEquals(1, properties.size());

        assertEquals("value1", properties.getProperty("key"));
    }

    @Test
    public void testAsProperties_XML() throws Exception {
        Properties properties = PropertiesHandler.asProperties(
                "<parameters>\n" +
                "  <key>value1</key>\n" +
                "  <key2>value3</key2>\n" +
                "</parameters>"
        );
        assertNotNull(properties);
        assertEquals(2, properties.size());

        assertEquals("value1", properties.getProperty("key"));
        assertEquals("value3", properties.getProperty("key2"));
    }

    @Test
    public void testAsProperties_XML_WithChilds() throws Exception {
        Properties properties = PropertiesHandler.asProperties(
                "<parameters>\n" +
                "  <key>value1</key>\n" +
                "  <childs>" +
                "    <ck1>value31</ck1>\n" +
                "    <ck2>value32</ck2>\n" +
                "    <ck3>value33</ck3>\n" +
                "  </childs>" +
                "</parameters>"
        );
        assertNotNull(properties);
        System.out.println("properties = " + properties);
        assertEquals(4, properties.size());

        assertEquals("value1", properties.getProperty("key"));
        assertEquals("value31", properties.getProperty("childs.ck1"));
        assertEquals("value32", properties.getProperty("childs.ck2"));
        assertEquals("value33", properties.getProperty("childs.ck3"));
    }

    @Test
    public void testAsProperties_XML_WithNestedChilds() throws Exception {
        Properties properties = PropertiesHandler.asProperties(
                "<parameters>\n" +
                "  <key>value1</key>\n" +
                "  <childs>" +
                "    <child>" +
                "       <name>name31</name>" +
                "       <value>value31</value>" +
                "    </child>\n" +
                "    <child>" +
                "       <name>name32</name>" +
                "       <value>value32</value>" +
                "    </child>\n" +
                "  </childs>" +
                "</parameters>"
        );
        assertNotNull(properties);
        System.out.println("properties = " + properties);
        assertEquals(6, properties.size());

        assertEquals("value1", properties.getProperty("key"));
        assertEquals("2", properties.getProperty("childs.length"));
        assertEquals("name31", properties.getProperty("childs.0.name"));
        assertEquals("value31", properties.getProperty("childs.0.value"));
        assertEquals("name32", properties.getProperty("childs.1.name"));
        assertEquals("value32", properties.getProperty("childs.1.value"));
    }

    @Test
    public void testAsProperties_XML_fromBfGExample() throws Exception {
        Properties properties = PropertiesHandler.asProperties(
                "<parameters>\n" +
                        "  <cloudBufferWidth>2</cloudBufferWidth>\n" +
                        "  <computeCloudBuffer>true</computeCloudBuffer>\n" +
                        "  <CHLexp>1.04</CHLexp>\n" +
                        "  <CHLfak>21.0</CHLfak>\n" +
                        "  <netSet>C2RCC-Nets</netSet>\n" +
                        "  <elevation>0.001</elevation>\n" +
                        "  <outputRtoa>false</outputRtoa>\n" +
                        "  <outputIops>false</outputIops>\n" +
                        "  <outputUncertainties>false</outputUncertainties>\n" +
                        "  <usertag></usertag>\n" +
                        "  <salinity>0.0001</salinity>\n" +
                        "  <temperature>15.0</temperature>\n" +
                        "  <waterRadiometricThreshold>0.0</waterRadiometricThreshold>\n" +
                        "  <TSMfakBpart>1.72</TSMfakBpart>\n" +
                        "  <TSMfakBwit>3.1</TSMfakBwit>\n" +
                        "  <validPixelExpression>not pixel_classif_flags.IDEPIX_INVALID and not pixel_classif_flags.IDEPIX_CLOUD and WATER_RADIOMETRIC == 1</validPixelExpression>\n" +
                        "  <doReproject>true</doReproject>\n" +
                        "  <crs>EPSG:25832</crs>\n" +
                        "  <pixelSize>60</pixelSize>\n" +
                        "  <resampling>Nearest</resampling>\n" +
                        "  <var1expression></var1expression>\n" +
                        "  <var2expression></var2expression>\n" +
                        "  <var3expression></var3expression>\n" +
                        "</parameters>"
        );
        assertNotNull(properties);
        System.out.println("properties = " + properties);
        assertEquals(19, properties.size());
    }
}
