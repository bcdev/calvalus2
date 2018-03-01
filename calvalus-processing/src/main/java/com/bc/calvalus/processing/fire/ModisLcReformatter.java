package com.bc.calvalus.processing.fire;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ModisLcReformatter {

    public static void main(String[] args) throws IOException {

        Files.list(Paths.get("C:\\ssd\\modis-lc\\"))
                .filter(p -> p.toString().endsWith("tif") && !Files.exists(Paths.get(p.toString().replace("tif", "nc"))))
                .forEach(p -> {
                            try {
                                System.out.println("Handling " + p.toString());
                                Product product = ProductIO.readProduct(p.toFile());
                                ProductIO.writeProduct(product, "c:\\ssd\\modis-lc\\" + p.getFileName().toString().replace("tif", "nc"), "NetCDF4-CF");
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                );
    }
}
