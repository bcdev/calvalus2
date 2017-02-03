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

package com.bc.calvalus.inventory;

import org.esa.snap.core.datamodel.ProductData;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DefaultInventoryServiceTest {

    @Test
    public void testReadProductSetFromCsv() throws Exception {
        String csv = "MER_RR__1;MERIS RR L1b 2004-2008;eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1;2004-01-01;2008-12-31;null;null\n" +
                     "MER_RR__1;MERIS RR L1b 2004;eodata/MER_RR__1P/r03/2004/${MM}/${dd}/.*.N1;2004-01-01;2004-12-31;null;null\n" +
                     "MER_RR__1;MERIS RR L1b 2005;eodata/MER_RR__1P/r03/2005/${MM}/${dd}/.*.N1;2005-01-01;2005-12-31;null;null\n";
        List<ProductSet> productSets = DefaultInventoryService.readProductSetFromCsv(
                new ByteArrayInputStream(csv.getBytes()));
        assertNotNull(productSets);
        assertEquals(3, productSets.size());
        assertEquals("MERIS RR L1b 2004", productSets.get(1).getName());
        assertEquals("eodata/MER_RR__1P/r03/2004/${MM}/${dd}/.*.N1", productSets.get(1).getPath());
        assertEquals(ProductData.UTC.createDateFormat("yyyy-MM-dd").parse("2004-01-01"),
                     productSets.get(1).getMinDate());
        assertEquals(ProductData.UTC.createDateFormat("yyyy-MM-dd").parse("2004-12-31"),
                     productSets.get(1).getMaxDate());
    }
}