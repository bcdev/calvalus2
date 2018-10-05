package com.bc.calvalus.processing.fire.format.grid.s2;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.junit.Test;

import java.awt.*;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class S2FireGridDataSourceTest {


    @Test
    public void testGetProductJd() throws Exception {
        assertEquals(50, S2FireGridDataSource.getProductJD(new Product("BA-T31NBJ-20160219T101925", "miau")));
    }

    @Test
    public void testGetTargetPixel() {
        System.out.println(S2FireGridDataSource.getTargetPixel(0, 0, 32, 33, 4, 3, 32, 4));
    }

    @Test
    public void name() throws IOException {
        Product product = ProductIO.readProduct("C:\\ssd\\s2-analysis\\20160101-ESACCI-L3S_FIRE-BA-MSI-AREA_h42v17-fv1.1-JD.tif");
        float lon0 = 30.0F;
        float lat0 = 4.0F;
        Geometry geometry;
        try {
            geometry = new WKTReader().read(String.format("POLYGON ((%s %s, %s %s, %s %s, %s %s, %s %s))",
                    lon0, lat0,
                    lon0 + 1, lat0,
                    lon0 + 1, lat0 + 1,
                    lon0, lat0 + 1,
                    lon0, lat0));
        } catch (ParseException e) {
            throw new IllegalStateException(e);
        }

        Rectangle rectangle = SubsetOp.computePixelRegion(product, geometry, 0);
        System.out.println(rectangle);
    }
}