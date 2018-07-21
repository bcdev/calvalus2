package com.bc.calvalus.processing.fire.format.pixel.modis;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.common.reproject.ReprojectionOp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class LcContinentSplitter {

    public static void main(String[] args) throws IOException {

        Map<String, String> continents = new HashMap<>();
        continents.put("AREA_1", "north_america"); // south_america-2005.nc
        continents.put("AREA_2", "south_america");
        continents.put("AREA_3", "europe");
        continents.put("AREA_4", "asia");
        continents.put("AREA_5", "africa");
        continents.put("AREA_6", "australia");

        Files.list(Paths.get("c:\\temp\\2.0.7")).forEach(
                p -> {
                    Product lcProduct = getLcProduct(p);
                    getRefProducts().forEach(
                            rp -> {
                                System.out.println(p);
                                String c = continents.get(rp.getFileLocation().getName().split("MODIS-")[1].split("-")[0]);
                                String y = p.getFileName().toString().split("P1Y-")[1].split("-")[0];
                                String filename = c + "_" + y + ".nc";
                                System.out.println("Handling " + c + "/" + y);

                                ReprojectionOp reprojectionOp = new ReprojectionOp();
                                reprojectionOp.setSourceProduct(lcProduct);
                                reprojectionOp.setParameterDefaultValues();
                                reprojectionOp.setSourceProduct("collocateWith", rp);

                                Product reprojectedProduct = reprojectionOp.getTargetProduct();
                                String filePath = "d:\\workspace\\fire-cci\\splitted-lc-data\\modis-pixel-v51\\" + filename;
                                if (Files.exists(Paths.get(filePath))) {
                                    return;
                                }
                                writeProduct(reprojectedProduct, c, y, filePath);
                            }
                    );
                }
        );


    }

    private static void writeProduct(Product collocatedProduct, String c, String y, String filePath) {
        try {
            ProductIO.writeProduct(collocatedProduct, filePath, "NetCDF4-CF");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Stream<Product> getRefProducts() {
        try {
            return Files.list(Paths.get("c:\\temp\\2.0.7\\ref")).map(path -> {
                try {
                    return ProductIO.readProduct(path.toFile());
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Product getLcProduct(Path p) {
        try {
            return ProductIO.readProduct(p.toFile());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
