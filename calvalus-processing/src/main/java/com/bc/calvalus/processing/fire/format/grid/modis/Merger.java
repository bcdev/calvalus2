package com.bc.calvalus.processing.fire.format.grid.modis;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.ProductUtils;

import java.io.IOException;

public class Merger {

    public static void main(String[] args) throws IOException {
        if (args.length != 5) {
            System.out.println("Usage:");
            System.out.println("    Merger <output> <path to classification> <path to uncertainty> <path to num_obs_1> <path to num_obs_2>");
            System.exit(-1);
        }

        String output = args[0];
        String classificationProductPath = args[1];
        String uncertaintyProductPath = args[2];
        String numObs1ProductPath = args[3];
        String numObs2ProductPath = args[4];

        Product classificationProduct = ProductIO.readProduct(classificationProductPath);
        Product numObs1Product = ProductIO.readProduct(numObs1ProductPath);
        Product numObs2Product = ProductIO.readProduct(numObs2ProductPath);

        Product result = new Product("Merged burned area information", "fire-cci-merged-modis", classificationProduct.getSceneRasterWidth(), classificationProduct.getSceneRasterHeight());
        ProductUtils.copyBand("band_1", classificationProduct, "classification", result, true);
        ProductUtils.copyBand("band_1", numObs1Product, "numObs1", result, true);
        ProductUtils.copyBand("band_1", numObs2Product, "numObs2", result, true);

        if (!uncertaintyProductPath.equals("dummy")) {
            Product uncertaintyProduct = ProductIO.readProduct(uncertaintyProductPath);
            ProductUtils.copyBand("band_1", uncertaintyProduct, "uncertainty", result, true);
        }

        ProductIO.writeProduct(result, output, "NetCDF4-CF");
    }

}
