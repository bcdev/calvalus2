/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.sentinel2;

import org.junit.Test;

import static org.junit.Assert.*;

public class GranuleSplitterTest {

    private static final String E1 = "S2A_OPER_PRD_MSIL1C_PDMC_20160215T121255_R093_V20160214T093550_20160214T093550.SAFE/";
    private static final String E2 = E1 + "GRANULE/";
    private static final String E3 = E2 + "S2A_OPER_MSI_L1C_TL_SGS__20160214T150139_A003379_T33PVP_N02.01/";
    private static final String E4 = E3 + "S2A_OPER_MTD_L1C_TL_SGS__20160214T150139_A003379_T33PVP.xml";

    @Test
    public void testDetectGranule() throws Exception {
        assertNull(GranuleSplitter.detectGranule(E1));
        assertNull(GranuleSplitter.detectGranule(E2));

        assertNotNull(GranuleSplitter.detectGranule(E3));
        assertNotNull(GranuleSplitter.detectGranule(E4));

        assertEquals("_T33PVP" , GranuleSplitter.detectGranule(E3));
        assertEquals("_T33PVP" , GranuleSplitter.detectGranule(E4));
    }

    @Test
    public void testGranuleSpec() throws Exception {
        GranuleSplitter.GranuleSpec spec = GranuleSplitter.GranuleSpec.parse(E3);
        assertNotNull(spec);
        assertEquals("S2A_OPER_PRD_MSIL1C_PDMC_20160214T150139_R093_V20160214T093550_20160214T093550_T33PVP", spec.getTopDirName());
        assertEquals("S2A_OPER_MSI_L1C_TL_SGS__20160214T150139_A003379_T33PVP_N02.01", spec.getGranuleName());
        assertEquals("_T33PVP", spec.getTileId());
        assertEquals("20160214T150139", spec.getGranuleProcessingTime());

        String oldXml = "S2A_OPER_MTD_SAFL1C_PDMC_20160215T121255_R093_V20160214T093550_20160214T093550.xml";
        String newXml = "S2A_OPER_MTD_SAFL1C_PDMC_20160214T150139_R093_V20160214T093550_20160214T093550_T33PVP.xml";
        assertEquals(newXml, spec.convertXmlName(oldXml));

//        String granule = "_T33PVP";
//        String newName = "S2A_OPER_PRD_MSIL1C_PDMC_20160214T150139_R093_V20160214T093550_20160214T093550_T33PVP";
//        assertEquals(newName, GranuleSplitter.getNewProductDirectoryName(E3, granule));
    }
}