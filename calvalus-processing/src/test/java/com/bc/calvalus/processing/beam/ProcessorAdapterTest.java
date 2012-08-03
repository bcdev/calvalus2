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

package com.bc.calvalus.processing.beam;

import org.junit.Test;

import static com.bc.calvalus.processing.JobUtils.createGeometry;
import static com.bc.calvalus.processing.beam.ProcessorAdapter.isGlobalCoverageGeometry;
import static org.junit.Assert.*;

/**
 * @author Norman
 */
public class ProcessorAdapterTest {

    @Test
    public void testIsGlobeCoverageGeometry() throws Exception {
        assertTrue(isGlobalCoverageGeometry(createGeometry("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))")));
        assertTrue(isGlobalCoverageGeometry(createGeometry("POLYGON((-180 -90, -180 90, 180 90,  180 -90,  -180 -90))")));
        assertFalse(isGlobalCoverageGeometry(createGeometry("POLYGON((-180 -90, 180 -90, 180 89, -180 89, -180 -90))")));
        assertFalse(isGlobalCoverageGeometry(createGeometry("POLYGON((-180 -90, 80 -90, 80 89, -180 89, -180 -90))")));
    }
}
