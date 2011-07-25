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

package com.bc.calvalus.production;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ProductionWriterTest {

    private ProductionWriter productionWriter;

    @Before
    public void setUp() throws Exception {
        Production production = new Production("9A3F", "Toasting", null,
                                               false, new ProductionRequest("test", "ewa", "a", "a1", "b", "b2"),
                                               new TestWorkflowItem(null));

        productionWriter = new ProductionWriter(production);
    }

    @Test
    public void testAsXml() throws Exception {
        String xml = productionWriter.asXML();

        assertNotNull(xml);
        assertEquals(320, xml.length());
        assertTrue(xml.contains("<id>9A3F</id>"));
        assertTrue(xml.contains("<name>Toasting</name>"));
    }

    @Test
    public void testAsHtml() throws Exception {
        String html = productionWriter.asHTML();

        assertNotNull(html);
        assertEquals(2168, html.length());
        assertTrue(html.contains("<td align=\"left\">9A3F</td>"));
    }

}
