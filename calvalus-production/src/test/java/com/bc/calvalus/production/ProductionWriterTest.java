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

import org.junit.Test;

import static org.junit.Assert.*;

public class ProductionWriterTest {

    @Test
    public void testXml() throws Exception {
        Production production = new Production("9A3F", "Toasting", null,
                                               false, new ProductionRequest("test", "ewa"),
                                               new TestWorkflowItem(null));

        String xml = ProductionWriter.asXML(production);

        assertNotNull(xml);
        assertEquals(207, xml.length());
        assertTrue(xml.contains("<id>9A3F</id>"));
        assertTrue(xml.contains("<name>Toasting</name>"));
    }
}
