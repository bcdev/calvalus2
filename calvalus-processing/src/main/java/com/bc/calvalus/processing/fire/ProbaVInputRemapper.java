package com.bc.calvalus.processing.fire;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.datamodel.Product;
import org.vito.probavbox.dataio.probav.ProbaVProductReaderPlugIn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ProbaVInputRemapper {

    public static void main(String[] args) throws IOException {
        Files.list(Paths.get("c:\\ssd\\proba-v"))
                .filter(p -> p.getFileName().toString().endsWith("HDF5"))
                .forEach(p -> {
                    try {
                        ProductReader reader = new ProbaVProductReaderPlugIn().createReaderInstance();
                        Product product = reader.readProductNodes(p.toFile(), null);
                        product.setProductReader(reader);
                        System.out.println("Handling " + p.toString() + "...");
                        String targetPath = p.toString().replace("HDF5", "tif").replace("c:\\ssd", "d:\\");
                        ProductIO.writeProduct(product, targetPath, "GeoTIFF");
                        System.out.println("...done.");
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                });
    }

}
