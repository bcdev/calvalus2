package com.bc.calvalus.processing.fire.format.grid.modis;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

import java.io.IOException;

public class Cleaner {

    public static void main(String[] args) throws IOException {
        Product product = ProductIO.readProduct("C:\\ssd\\modis-analysis\\20080101-ESACCI-L4_FIRE-BA-MODIS-fv1.1.nc");
        int width = product.getSceneRasterWidth();
        int height = product.getSceneRasterHeight();
        float[] burnableFraction = new float[width * height];
        float[] obsFraction = new float[width * height];
        product.getBand("fraction_of_burnable_area").readPixels(0, 0, width, height, burnableFraction);
        product.getBand("fraction_of_observed_area").readPixels(0, 0, width, height, obsFraction);

        for (int i = 0; i < obsFraction.length; i++) {
            float obs = obsFraction[i];
            float burn = burnableFraction[i];
            if (obs > 0 && burn == 0.0) {
                obsFraction[i] = 0;
            }
        }

        product.getBand("fraction_of_observed_area").setRasterData(new ProductData.Float(obsFraction));
        ProductIO.writeProduct(product, "C:\\ssd\\modis-analysis\\cleaned.nc", "NetCDF4-CF");

    }

}
