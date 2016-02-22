package com.bc.calvalus.processing.beam;

import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author thomas
 */
public class CalvalusProductIOTest {

    @Test
    public void testSetDate() throws Exception {
        Product product = new Product("name", "type", 10, 10);
        CalvalusProductIO.setDateToMerisSdrProduct(product, "CCI-Fire-MERIS-SDR-L3-300m-v1.0-2008-12-07-v04h07.nc");
        long expected = ProductData.UTC.parse("2008-12-07", "yyyy-MM-dd").getAsDate().getTime();
        long actual = product.getStartTime().getAsDate().getTime();
        assertEquals(expected, actual);

    }
}