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

import static org.junit.Assert.assertEquals;

public class ScriptGeneratorTest {

    @Test
    public void testCreateResultName() throws Exception {
        assertEquals("cmdline", ScriptGenerator.createResultName("l2gen-cmdline.vm", "l2gen"));
        assertEquals("config.txt", ScriptGenerator.createResultName("l2gen-config.txt.vm", "l2gen"));
        assertEquals("config.txt", ScriptGenerator.createResultName("l2gen-config.txt", "l2gen"));
    }

}
