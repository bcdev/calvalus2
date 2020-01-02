package com.bc.calvalus.processing.fire.format.grid.modis;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.common.reproject.ReprojectionOp;

import java.io.IOException;

public class LcReprojector {

    public static void main(String[] args) throws IOException {
        String refProductFilename = "D:\\workspace\\fire-cci\\modis-for-lc\\h09v02.hdf";
        Product refProduct = ProductIO.readProduct(refProductFilename);
        for (int year = 2000; year <= 2015; year++) {
            System.out.println(year);
            Product lcProduct = ProductIO.readProduct("D:\\workspace\\fire-cci\\lc-data-to-split\\2.0.7\\ESACCI-LC-L4-LCCS-Map-300m-P1Y-" + year + "-v2.0.7.tif");
//            CollocateOp collocateOp = new CollocateOp();
//            collocateOp.setMasterProduct(refProduct);
//            collocateOp.setSlaveProduct(lcProduct);
//            collocateOp.setParameterDefaultValues();
//
//            Product reprojectedProduct = collocateOp.getTargetProduct();
            ReprojectionOp reprojectionOp = new ReprojectionOp();
            reprojectionOp.setSourceProduct("collocationProduct", refProduct);
            reprojectionOp.setSourceProduct(lcProduct);
            reprojectionOp.setParameterDefaultValues();
            Product targetProduct = reprojectionOp.getTargetProduct();
            targetProduct.setSceneGeoCoding(null);
            targetProduct.removeBand(targetProduct.getBand("lat"));
            targetProduct.removeBand(targetProduct.getBand("lon"));
            targetProduct.getBand("band_1").setName("lccs_class");
            ProductIO.writeProduct(targetProduct, "D:\\workspace\\fire-cci\\modis-for-lc\\h09v02-" + year + ".nc", "NetCDF4-CF");
        }
    }
}
