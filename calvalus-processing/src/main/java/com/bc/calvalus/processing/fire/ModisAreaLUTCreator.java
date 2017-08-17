package com.bc.calvalus.processing.fire;

import com.bc.calvalus.processing.fire.format.grid.AreaCalculator;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Splits LC map into tiles relevant for Fire-CCI processing.
 */
public class ModisAreaLUTCreator {

    public static void main(String[] args) throws IOException {
//        String modisInputPath = "D:\\workspace\\fire-cci\\modis-for-lc";
//        String outputPath = "D:\\workspace\\fire-cci\\modis-area-lut\\";

        String modisInputPath = args[0];
        String outputPath = args[1];

        Files.list(Paths.get(modisInputPath)).filter(path -> path.toString().endsWith(".hdf")).forEach(path -> {
            try {
                Product reference = ProductIO.readProduct(path.toFile());
                String tile = reference.getName();
                String outputFilename = outputPath + "\\areas-" + tile + ".nc";
                if (Files.exists(Paths.get(outputFilename))) {
                    System.out.println("Already existing tile: " + tile);
                    return;
                }

                Product areaLut = new Product("areas-" + tile, "modis-area-lut", 4800, 4800);
                Band band = areaLut.addBand("areas", ProductData.TYPE_FLOAT64);

                AreaCalculator areaCalculator = new AreaCalculator(reference.getSceneGeoCoding());

                double[] data = new double[4800 * 4800];
                int pixelIndex = 0;
                for (int y = 0; y < 4800; y++) {
                    for (int x = 0; x < 4800; x++) {
                        double pixelSize = areaCalculator.calculatePixelSize(x, y, reference.getSceneRasterWidth() - 1, reference.getSceneRasterHeight() - 1);
                        data[pixelIndex] = pixelSize;
                        pixelIndex++;
                    }
                }
                band.setData(new ProductData.Double(data));

                ProductIO.writeProduct(areaLut, outputFilename, "NetCDF4-CF");

            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }
}
