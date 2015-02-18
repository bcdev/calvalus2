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

package com.bc.calvalus.processing;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.*;

public class ProcessorAdapterTest {

    @Test
    public void testGetDatePart() throws Exception {
        assertEquals("2005/06/09", ProcessorAdapter.getDatePart(new Path("/foo/2005/06/09/MER.N1")));
        assertEquals("2005/06/09", ProcessorAdapter.getDatePart(new Path("foo/2005/06/09/MER.N1")));
        assertEquals("2005/06/09", ProcessorAdapter.getDatePart(new Path("/2005/06/09/MER.N1")));
        assertEquals("2005/06/09", ProcessorAdapter.getDatePart(new Path("2005/06/09/MER.N1")));
        assertNull(ProcessorAdapter.getDatePart(new Path("/06/09/MER.N1")));
        assertNull(ProcessorAdapter.getDatePart(new Path("/09/MER.N1")));
        assertNull(ProcessorAdapter.getDatePart(new Path("09/MER.N1")));
        assertNull(ProcessorAdapter.getDatePart(new Path("/MER.N1")));
        assertNull(ProcessorAdapter.getDatePart(new Path("MER.N1")));
    }

}