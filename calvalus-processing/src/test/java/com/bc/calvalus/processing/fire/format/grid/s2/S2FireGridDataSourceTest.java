package com.bc.calvalus.processing.fire.format.grid.s2;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class S2FireGridDataSourceTest {

    @Test
    public void testGetProductJd() throws Exception {
        assertEquals(50, S2FireGridDataSource.getProductJD(new Product("BA-T31NBJ-20160219T101925", "miau")));
    }

    @Test
    public void name() throws Exception {
        ProductData.Float jdData = new ProductData.Float(1);
        Product product = ProductIO.readProduct("C:\\ssd\\BA-T32QQF-20160101T095719.nc");
        product.getBand("JD").readRasterData(4961, 5394, 1, 1, jdData);
        assertEquals(-100, jdData.getElemFloatAt(0), 1E-10);

    }
}