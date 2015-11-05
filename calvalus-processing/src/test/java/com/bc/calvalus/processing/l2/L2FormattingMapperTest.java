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

import com.bc.calvalus.processing.JobConfigNames;
import org.apache.hadoop.mapred.JobConf;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class L2FormattingMapperTest {

    @Test
    public void testGetNewProductname() throws Exception {

        assertEquals("MER_RR__1P2000", newName("L2_of_MER_RR__1P2000", "L2_of_(.+)", "$1"));
        assertEquals("MER_SDR_1P2000", newName("L2_of_MER_RR__1P2000", "L2_of_MER_RR_(.+)", "MER_SDR$1"));
        assertEquals("MER_RR__1P2000_cc", newName("L2_of_MER_RR__1P2000", "L2_of_(.+)", "$1_cc"));
        //cc
        assertEquals("MER_RR__CCL1P_20060708_1234567",
                     newName("L2_of_MER_RR__1PNMAP20060708_1234567", "L2_of_(MER_..._)1.....(.+)", "$1CCL1P_$2"));
        assertEquals("MER_FSG_CCL1P_20060708_1234567",
                     newName("L2_of_MER_FSG_1PNMAP20060708_1234567", "L2_of_(MER_..._)1.....(.+)", "$1CCL1P_$2"));

        //lc sdr
        assertEquals("MER_FSG_SDR_20050708_112233_042",
                     newName("L2_of_MER_FSG_1PNUPA20050708_112233_042", "L2_of_(MER_FSG)_1.....(.+)", "$1_SDR_$2"));

        // freshmon
        assertEquals("CHL_Northsea_BC_20060708_123456",
                     newName("L2_of_MER_RR__1PNMAP20060708_123456_0815", "L2_of_MER_..._1.....(........_......).*",
                             "CHL_Northsea_BC_$1"));

    }

    @Test(expected = IllegalStateException.class)
    public void testCreateBandSubsetParameterWhenBandIsMissing() throws Exception {
        JobConf jobConfig = new JobConf();
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_BANDLIST, "Band_1,Band_2,Band_3");
        Product targetProduct = new Product("name", "type", 10, 10);
        targetProduct.addBand("Band_1", ProductData.TYPE_INT16);
        targetProduct.addBand("Band_3", ProductData.TYPE_INT16);
        L2FormattingMapper.createBandSubsetParameter(targetProduct, jobConfig);
    }

    @Test
    public void testCreateBandSubsetParameter() throws Exception {
        JobConf jobConfig = new JobConf();
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_BANDLIST, "Band_1,Band_2,Band_3");
        Product targetProduct = new Product("name", "type", 10, 10);
        targetProduct.addBand("Band_1", ProductData.TYPE_INT16);
        targetProduct.addBand("Band_2", ProductData.TYPE_INT16);
        targetProduct.addBand("Band_3", ProductData.TYPE_INT16);
        Map<String, Object> bandSubsetParameter = L2FormattingMapper.createBandSubsetParameter(targetProduct,
                                                                                               jobConfig);
        String names = (String) bandSubsetParameter.get("bandNames");
        assertNotNull(names);
        String[] bandNames = names.split(",");
        assertEquals(3, bandNames.length);
        assertEquals("Band_1", bandNames[0]);
        assertEquals("Band_2", bandNames[1]);
        assertEquals("Band_3", bandNames[2]);

    }

    private static String newName(String productName, String regex, String replacement) {
        return L2FormattingMapper.getNewProductName(productName, regex, replacement);
    }
}
