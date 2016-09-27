package com.bc.calvalus.processing.fire.format.pixel;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.junit.Test;

import java.io.File;

/**
 * Created by Thomas on 27.09.2016.
 */
public class PixelMapperTest {

    @Test
    public void name() throws Exception {

        File[] files = new File("D:\\workspace\\fire-cci\\testdata\\out").listFiles();
        for (int i = 0; i < files.length; i++) {
            File f = files[i];
            System.out.println("Processing " + (i + 1) + "/" + files.length + "...");
            Product product = ProductIO.readProduct(f);
            ProductIO.writeProduct(product, product.getFileLocation().getAbsolutePath().replace("tif", "nc"), "GeoTiff");
            product.dispose();
            System.out.println("...done.");
        }

    }
}