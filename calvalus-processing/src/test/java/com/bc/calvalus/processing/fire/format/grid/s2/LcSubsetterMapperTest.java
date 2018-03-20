package com.bc.calvalus.processing.fire.format.grid.s2;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LcSubsetterMapperTest {

    @Test
    public void name() throws Exception {
        Product lcProduct = ProductIO.readProduct("C:\\ssd\\s2-lc\\ESACCI-LC-L4-LC10-Map-20m-P1Y-2016-v1.0.tif");
        Files.list(Paths.get("D:\\workspace\\temp\\for-s2-lc")).filter(p -> p.getFileName().toString().endsWith("nc")).forEach(
                p -> {
                    try {
                        String pathname = "D:\\workspace\\temp\\for-s2-lc\\" + p.getFileName().toString();
                        System.out.println(pathname);
                        String tile = pathname.split("-T")[1].substring(0, 5);
                        if (Files.exists(Paths.get("lc-2010-T" + tile + ".nc"))) {
                            return;
                        }
                        LcSubsetterMapper.subset(lcProduct, new File(pathname), tile);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
        );
    }
}