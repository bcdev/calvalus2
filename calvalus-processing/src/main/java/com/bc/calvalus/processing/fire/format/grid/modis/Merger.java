package com.bc.calvalus.processing.fire.format.grid.modis;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.ProductUtils;
import org.esa.snap.dataio.geotiff.GeoTiffProductReaderPlugIn;

import java.io.File;
import java.io.IOException;

public class Merger {

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.out.println("Usage:");
            System.out.println("    Merger <output> <path to classification> <path to uncertainty> <path to num_obs>");
            System.exit(-1);
        }

        String output = args[0];
        String classificationProductPath = args[1];
        String uncertaintyProductPath = args[2];
        String numObsProductPath = args[3];

        ProductReader productReader = new GeoTiffProductReaderPlugIn().createReaderInstance();
        ProductReader productReader2 = new GeoTiffProductReaderPlugIn().createReaderInstance();
        ProductReader productReader3 = new GeoTiffProductReaderPlugIn().createReaderInstance();

        Product classificationProduct = productReader.readProductNodes(new File(classificationProductPath), null);
        Product numObs1Product = productReader2.readProductNodes(new File(numObsProductPath), null);

        Product result = new Product("Merged burned area information", "fire-cci-merged-modis", classificationProduct.getSceneRasterWidth(), classificationProduct.getSceneRasterHeight());
        ProductUtils.copyBand("band_1", classificationProduct, "classification", result, true);
        ProductUtils.copyBand("band_1", numObs1Product, "numObs1", result, true);

        if (!uncertaintyProductPath.equals("dummy")) {
            Product uncertaintyProduct = productReader3.readProductNodes(uncertaintyProductPath, null);
            ProductUtils.copyBand("band_1", uncertaintyProduct, "uncertainty", result, true);
        }

        ProductIO.writeProduct(result, output, "NetCDF4-CF");
    }

}
