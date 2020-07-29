package com.bc.calvalus.processing.fire.format.grid.modis;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.ProductUtils;
import org.esa.snap.dataio.geotiff.GeoTiffProductReaderPlugIn;
import org.geotools.xml.xsi.XSISimpleTypes;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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

    /*
            for (String month : new String[]{"10", "11", "12"}) {
            Set<String> tiles = new HashSet<>();
            Files.list(Paths.get("C:\\ssd\\fire\\modis\\fix\\" + month)).forEach(p -> tiles.add(p.toString().split("_")[2]));
            for (String tile : tiles) {
                final Object[] tilePaths = Files.list(Paths.get("C:\\ssd\\fire\\modis\\fix\\" + month)).filter(p -> p.toString().contains(tile)).toArray();
                String classificationProductPath = null;
                String uncertaintyProductPath = null;
                String numObsProductPath = null;
                for (Object tilePath : tilePaths) {
                    if (tilePath.toString().contains("Classification")) {
                        classificationProductPath = tilePath.toString();
                    } else if (tilePath.toString().contains("Observed")) {
                        numObsProductPath = tilePath.toString();
                    } else if (tilePath.toString().contains("Uncertainty")) {
                        uncertaintyProductPath = tilePath.toString();
                    } else {
                        throw new IllegalStateException("Invalid file " + tilePath);
                    }
                }
                System.out.println("Running for " + tile + ", " + month);
                run("c:\\ssd\\fire\\modis\\fix\\burned_" + tile + "_2019_" + month + ".nc", classificationProductPath, uncertaintyProductPath, numObsProductPath);
            }
        }
     */

}
