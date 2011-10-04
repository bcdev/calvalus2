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

package com.bc.calvalus.processing.l2;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class L2FormatingMapperTest {

    @Test
    public void testGetNewProductname() throws Exception {

        assertEquals("MER_RR__1P2000", newName("L2_of_MER_RR__1P2000", "L2_of_(.+)", "$1"));
        assertEquals("MER_SDR_1P2000", newName("L2_of_MER_RR__1P2000", "L2_of_MER_RR_(.+)", "MER_SDR$1"));
        assertEquals("MER_RR__1P2000_cc", newName("L2_of_MER_RR__1P2000", "L2_of_(.+)", "$1_cc"));
        //cc
        assertEquals("MER_RR__CCL1P_20060708_1234567", newName("L2_of_MER_RR__1PNMAP20060708_1234567", "L2_of_(MER_..._)1.....(.+)", "$1CCL1P_$2"));
        assertEquals("MER_FSG_CCL1P_20060708_1234567", newName("L2_of_MER_FSG_1PNMAP20060708_1234567", "L2_of_(MER_..._)1.....(.+)", "$1CCL1P_$2"));
    }

    private static String newName(String productName, String regex, String replacement) {
        return L2FormatingMapper.getNewProductname(productName, regex, replacement);
    }
}
