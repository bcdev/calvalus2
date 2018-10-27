package com.bc.calvalus.processing.fire.format.grid.avhrr;

import com.bc.calvalus.processing.fire.format.grid.CollocationOp;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Ignore;
import org.junit.Test;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.File;
import java.io.IOException;

public class CollocationOpTest {

    @Ignore
    @Test
    public void name() throws IOException {

        File lcFile = new File("C:\\ssd\\avhrr\\ESACCI-LC-L4-LCCS-Map-300m-P1Y-1999-v2.0.7.tif");
        Product product = ProductIO.readProduct(lcFile);

        Product dummyCrsProduct = new Product("dummy", "dummy", 7200, 3600);
        dummyCrsProduct.addBand("dummy", "1");
        try {
            dummyCrsProduct.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 7200, 3600, -180.0, 90.0, 360.0 / 7200.0, 180.0 / 3600.0, 0.0, 0.0));
        } catch (FactoryException | TransformException e) {
            throw new IllegalStateException("Programming error, see nested exception", e);
        }
        CollocationOp collocateOp = new CollocationOp();
//        CollocateOp collocateOp = new CollocateOp();
        collocateOp.setMasterProduct(dummyCrsProduct);
        collocateOp.setSlaveProduct(product);
        collocateOp.setParameterDefaultValues();

        Product reprojectedProduct = collocateOp.getTargetProduct();
        ProductIO.writeProduct(reprojectedProduct, "c:\\ssd\\avhrr\\majority-LC2.nc", "NetCDF4-CF");
    }
}