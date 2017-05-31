package com.bc.calvalus.processing.fire.format.grid.modis;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.ProductUtils;

import java.io.IOException;

public class Merger {

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.out.println("Usage:");
            System.out.println("    Merger <output> <path to classification> <path to uncertainty> <path to doy>");
            System.exit(-1);
        }

        String output = args[0];
        String classificationProductPath = args[1];
        String uncertaintyProductPath = args[2];
        String doyProductPath = args[3];

        Product classificationProduct = ProductIO.readProduct(classificationProductPath);
        Product uncertaintyProduct = ProductIO.readProduct(uncertaintyProductPath);
        Product doyProduct = ProductIO.readProduct(doyProductPath);

        Product result = new Product("Merged burned area information", "fire-cci-merged-modis", classificationProduct.getSceneRasterWidth(), classificationProduct.getSceneRasterHeight());
        ProductUtils.copyBand("band_1", classificationProduct, "classification", result, true);
        ProductUtils.copyBand("band_1", uncertaintyProduct, "uncertainty", result, true);
        ProductUtils.copyBand("band_1", doyProduct, "doy", result, true);
        ProductIO.writeProduct(result, output, "NetCDF4-CF");
    }

}
