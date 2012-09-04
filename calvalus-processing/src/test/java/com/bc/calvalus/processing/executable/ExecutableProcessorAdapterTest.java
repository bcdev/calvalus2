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
import java.util.Set;

import static org.junit.Assert.*;

public class ExecutableProcessorAdapterTest {


    @Test
    public void testAsProperties_KeyValue() throws Exception {
        Properties properties = ExecutableProcessorAdapter.asProperties("key = value1\n  key2   = value3");
        assertNotNull(properties);
        assertEquals(2, properties.size());

        assertEquals("value1", properties.getProperty("key"));
        assertEquals("value3", properties.getProperty("key2"));

    }

    @Test
    public void testAsProperties_XML() throws Exception {
        Properties properties = ExecutableProcessorAdapter.asProperties(
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
        Properties properties = ExecutableProcessorAdapter.asProperties(
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
        assertEquals(5, properties.size());

        assertEquals("value1", properties.getProperty("key"));
        assertEquals("3", properties.getProperty("childs.length"));
        assertEquals("value31", properties.getProperty("childs.0.ck1"));
        assertEquals("value32", properties.getProperty("childs.1.ck2"));
        assertEquals("value33", properties.getProperty("childs.2.ck3"));
    }

    @Test
    public void testAsProperties_XML_WithComplexChilds() throws Exception {
        Properties properties = ExecutableProcessorAdapter.asProperties(
                "<parameters>\n" +
                "  <key>value1</key>\n" +
                "  <childs>" +
                "    <ck1>" +
                "       <name>name31</name>" +
                "       <value>value31</value>" +
                "    </ck1>\n" +
                "    <ck2>value32</ck2>\n" +
                "    <ck3>value33</ck3>\n" +
                "  </childs>" +
                "</parameters>"
        );
        assertNotNull(properties);
        assertEquals(7, properties.size());

        assertEquals("value1", properties.getProperty("key"));
        assertEquals("3", properties.getProperty("childs.length"));
        assertEquals("2", properties.getProperty("childs.0.ck1.length"));
        assertEquals("name31", properties.getProperty("childs.0.ck1.0.name"));
        assertEquals("value31", properties.getProperty("childs.0.ck1.1.value"));
        assertEquals("value32", properties.getProperty("childs.1.ck2"));
        assertEquals("value33", properties.getProperty("childs.2.ck3"));
    }

}
