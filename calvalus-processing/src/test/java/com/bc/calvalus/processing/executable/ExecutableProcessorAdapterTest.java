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

public class ExecutableProcessorAdapterTest {


    @Test
    public void testAsProperties_KeyValue() throws Exception {
        Properties properties = ExecutableProcessorAdapter.asProperties("key = value1\n  key2   = value3");
        assertNotNull(properties);
        assertEquals(2, properties.size());

        String v = properties.getProperty("key");
        assertNotNull(v);
        assertEquals("value1", v);

        v = properties.getProperty("key2");
        assertNotNull(v);
        assertEquals("value3", v);

    }

    @Test
    public void testAsProperties_XML() throws Exception {
        Properties properties = ExecutableProcessorAdapter.asProperties(
                "<parameters>\n" +
                        "<key>value1</key>\n" +
                        "<key2>value3</key2>\n" +
                        "</parameters>"
        );
        String s = properties.toString();
        System.out.println("s = " + s);
        assertNotNull(properties);
        assertEquals(2, properties.size());

        String v = properties.getProperty("key");
        assertNotNull(v);
        assertEquals("value1", v);

        v = properties.getProperty("key2");
        assertNotNull(v);
        assertEquals("value3", v);

    }

}
