/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.ma.MAConfig;
import org.junit.Test;

import static org.junit.Assert.*;

public class MAProductionTypeTest {

    @Test
    public void testGetMAConfig() throws Exception {

    }

    @Test
    public void testParseVariableMappings() throws Exception {
        assertNull(MAProductionType.parseVariableMappings(null));
        assertNull(MAProductionType.parseVariableMappings(""));
        assertNull(MAProductionType.parseVariableMappings("ref"));
        assertNull(MAProductionType.parseVariableMappings("ref="));
        assertNull(MAProductionType.parseVariableMappings("ref1,ref2"));

        MAConfig.VariableMapping[] mappings = MAProductionType.parseVariableMappings("r1=s1");
        assertNotNull(mappings);
        assertEquals(1, mappings.length);
        assertEquals("r1", mappings[0].getReference());
        assertEquals("s1", mappings[0].getSatellite());

        mappings = MAProductionType.parseVariableMappings("r1=s1,r2=s2");
        assertNotNull(mappings);
        assertEquals(2, mappings.length);
        assertEquals("r1", mappings[0].getReference());
        assertEquals("s1", mappings[0].getSatellite());
        assertEquals("r2", mappings[1].getReference());
        assertEquals("s2", mappings[1].getSatellite());
    }
}