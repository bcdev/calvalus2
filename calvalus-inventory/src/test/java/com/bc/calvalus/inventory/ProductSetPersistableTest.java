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

package com.bc.calvalus.inventory;

import org.junit.Test;

import static org.junit.Assert.*;

public class ProductSetPersistableTest {

    @Test
    public void testConvertFromCSV_EdgeCases() throws Exception {
        assertNull(ProductSetPersistable.convertFromCSV(""));
        try {
            ProductSetPersistable.convertFromCSV(null);
            fail();
        } catch (NullPointerException expected) {
        }
    }

    @Test
    public void testConvertFromCSV_oldFormat() throws Exception {
        ProductSet productSet = ProductSetPersistable.convertFromCSV("MERIS RR L1b 2004-2008;eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1;2004-01-01;2008-12-31");
        assertNotNull(productSet);
        assertNull(productSet.getProductType());
        assertEquals("MERIS RR L1b 2004-2008", productSet.getName());
        assertEquals("eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1", productSet.getPath());
        assertEquals("2004-01-01", ProductSetPersistable.format(productSet.getMinDate()));
        assertEquals("2008-12-31", ProductSetPersistable.format(productSet.getMaxDate()));
        assertNull(productSet.getRegionName());
        assertNull(productSet.getRegionWKT());

        productSet = ProductSetPersistable.convertFromCSV("ps0;ps0;null;null");
        assertNotNull(productSet);
        assertNull(productSet.getProductType());
        assertEquals("ps0", productSet.getName());
        assertEquals("ps0", productSet.getPath());
        assertNull(productSet.getMinDate());
        assertNull(productSet.getMaxDate());
        assertNull(productSet.getRegionName());
        assertNull(productSet.getRegionWKT());
    }

    @Test
    public void testConvertFromCSV() throws Exception {
        ProductSet productSet = ProductSetPersistable.convertFromCSV("MER_RR__1;MERIS RR L1b 2004-2008;eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1;2004-01-01;2008-12-31;WesternEurope;polygon((-7 54, -7 38.5, 5.5 38.5, 5.5 54, -7 54))");
        assertNotNull(productSet);
        assertEquals("MER_RR__1", productSet.getProductType());
        assertEquals("MERIS RR L1b 2004-2008", productSet.getName());
        assertEquals("eodata/MER_RR__1P/r03/${yyyy}/${MM}/${dd}/.*.N1", productSet.getPath());
        assertEquals("2004-01-01", ProductSetPersistable.format(productSet.getMinDate()));
        assertEquals("2008-12-31", ProductSetPersistable.format(productSet.getMaxDate()));
        assertEquals("WesternEurope", productSet.getRegionName());
        assertEquals("polygon((-7 54, -7 38.5, 5.5 38.5, 5.5 54, -7 54))", productSet.getRegionWKT());

        productSet = ProductSetPersistable.convertFromCSV("null;ps0;ps0;null;null;null;null");
        assertNotNull(productSet);
        assertNull(productSet.getProductType());
        assertEquals("ps0", productSet.getName());
        assertEquals("ps0", productSet.getPath());
        assertNull(productSet.getMinDate());
        assertNull(productSet.getMaxDate());
        assertNull(productSet.getRegionName());
        assertNull(productSet.getRegionWKT());
    }

    @Test
    public void testConvertToCSV() throws Exception {
        ProductSet productSet = new ProductSet(null, "ps0", "ps0", null, null, null, null);
        String csv = ProductSetPersistable.convertToCSV(productSet);
        assertNotNull(csv);
        assertEquals("null;ps0;ps0;null;null;null;null", csv);
    }

}
