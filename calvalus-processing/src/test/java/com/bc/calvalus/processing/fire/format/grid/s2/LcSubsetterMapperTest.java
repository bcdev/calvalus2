package com.bc.calvalus.processing.fire.format.grid.s2;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LcSubsetterMapperTest {

    @Ignore
    @Test
    public void doSubsettingTestRun() throws Exception {
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

    /*
    @Test
    public void createQLs() throws Exception {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
        while (true) {

            Files.list(Paths.get("D:\\workspace\\fire-cci\\meetings\\2018-03-22_Progress-Meeting-08\\data"))
                    .filter(p -> p.getFileName().toString().contains("33PTK") && !p.getFileName().toString().startsWith(".") && p.getFileName().toString().endsWith(".nc"))
                    .forEach(p -> {
                        try {
                            if (Files.exists(Paths.get(p.toString() + ".png"))) {
                                System.out.println("Skipping " + p.toString());
                                return;
                            } else {
                                System.out.println("Quicklooking " + p.toString());
                            }
                            Product product = ProductIO.readProduct(p.toFile());
                            product.addBand("JD2", "JD == 999 ? -2 : JD == 998 ? -1 : JD == 997 ? -2 : JD");
                            Quicklooks.QLConfig qlConfig = new Quicklooks.QLConfig();
                            qlConfig.setCpdURL(Paths.get("C:\\Users\\Thomas\\.snap\\auxdata\\color_palettes\\fire-s2-ba-2.cpd").toUri().toURL().toString());
//                        qlConfig.setSubSamplingX(200);
//                        qlConfig.setSubSamplingY(200);
                            qlConfig.setBandName("JD2");
                            qlConfig.setImageType("png");
                            if (product.getBand("JD") == null) {
                                throw new IllegalStateException(product.getName());
                            }
                            RenderedImage image = QuicklookGenerator.createImage(null, product, qlConfig);
                            ImageIO.write(image, "PNG", new File(p.toString() + ".png"));

                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    });
            System.out.println("Waiting for 3 minutes...");
            Thread.sleep(3 * 60 * 1000);
        }
    }

    */

}