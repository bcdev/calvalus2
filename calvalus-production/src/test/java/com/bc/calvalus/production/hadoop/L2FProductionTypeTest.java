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

package com.bc.calvalus.production.hadoop;


import com.bc.calvalus.production.ProductionRequest;
import org.junit.Test;

import static org.junit.Assert.*;

public class L2FProductionTypeTest {
    @Test
    public void testCreateProductionName() throws Exception {
        ProductionRequest productionRequest = new ProductionRequest("L2F", "ewa",
                                                                    "minDate", "2005-01-01",
                                                                    "maxDate", "2005-01-31");
        String productionName = L2FProductionType.createProductionName(productionRequest);
        assertEquals("Level 2 Format NetCDF 2005-01-01 to 2005-01-31", productionName);

        productionRequest = new ProductionRequest("L2F", "ewa",
                                                  "minDate", "2005-01-01",
                                                  "maxDate", "2005-12-31",
                                                  "regionName", "northsea",
                                                  "outputFormat", "GeoTIFF");
        productionName = L2FProductionType.createProductionName(productionRequest);
        assertEquals("Level 2 Format GeoTIFF 2005-01-01 to 2005-12-31 (northsea)", productionName);
    }
}
