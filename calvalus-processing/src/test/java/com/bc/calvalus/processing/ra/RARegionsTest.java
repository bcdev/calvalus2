/*
 * Copyright (C) 2017 Brockmann Consult GmbH (info@brockmann-consult.de) 
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

package com.bc.calvalus.processing.ra;

import org.junit.Test;

import static org.junit.Assert.*;

public class RARegionsTest {
    
    private static final String WKT1 = "PROJCS[\"ETRS_1989_LAEA\",GEOGCS[\"GCS_ETRS_1989\",DATUM[\"D_ETRS_1989\",SPHEROID[\"GRS_1980\",6378137.0,298.257222101]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Lambert_Azimuthal_Equal_Area\"],PARAMETER[\"False_Easting\",4321000.0],PARAMETER[\"False_Northing\",3210000.0],PARAMETER[\"Central_Meridian\",10.0],PARAMETER[\"Latitude_Of_Origin\",52.0],UNIT[\"Meter\",1.0]]";
    private static final String WKT2a = "GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433],METADATA[\"World\",-180.0,-90.0,180.0,90.0,0.0,0.0174532925199433,0.0,1262]]"; 
    private static final String WKT2b = "GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]]"; 

    @Test
    public void test_removeMetadataFromWkt() throws Exception {
        assertEquals(WKT1, RARegions.removeMetadataFromWkt(WKT1));
        assertEquals(WKT2b, RARegions.removeMetadataFromWkt(WKT2a));

    }
}