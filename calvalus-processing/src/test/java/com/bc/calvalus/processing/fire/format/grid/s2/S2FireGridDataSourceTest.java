package com.bc.calvalus.processing.fire.format.grid.s2;

import org.esa.snap.core.datamodel.Product;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class S2FireGridDataSourceTest {


    @Test
    public void testGetProductJd() throws Exception {
        assertEquals(50, S2FireGridDataSource.getProductJD(new Product("BA-T31NBJ-20160219T101925", "miau")));
    }

    @Test
    public void testGetTargetPixel() {
        System.out.println(S2FireGridDataSource.getTargetPixel(3000, 1000, 30, 31, 4, 5, 31, 4.5));
    }
}