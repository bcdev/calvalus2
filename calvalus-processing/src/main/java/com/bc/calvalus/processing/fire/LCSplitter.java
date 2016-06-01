package com.bc.calvalus.processing.fire;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Splits LC map into tiles relevant for Fire-CCI processing.
 */
public class LCSplitter {

    private static final int RASTER_LENGTH = 3600;

    public static void main(String[] args) throws IOException {
        FileSystem fileSystem = FileSystems.getDefault();
        List<String> tiles = readTiles();

        String outputPath = "D:\\workspace\\fire-cci\\splitted-lc-data";
        String year = "2000";

        Path inputPath = fileSystem.getPath("C:\\temp\\formatting_development", "ESACCI-LC-L4-LCCS-Map-300m-P5Y-2000-v1.6.1.nc");
        Product product = ProductIO.readProduct(inputPath.toFile());

        List<Runnable> tasks = new ArrayList<>();
        for (String tile : tiles) {
            tasks.add(new Runnable() {
                @Override
                public void run() {
                    System.out.format("Handling tile %s\n", tile);
                    int[] sourcePixels = new int[RASTER_LENGTH * RASTER_LENGTH];
                    byte[] targetPixels = new byte[RASTER_LENGTH * RASTER_LENGTH];
                    Product tileProduct = new Product(tile, "lc", RASTER_LENGTH, RASTER_LENGTH);
                    Band lcclass = tileProduct.addBand("lcclass", ProductData.TYPE_INT8);
                    int x = getX(tile);
                    int y = getY(tile);
                    System.out.format("     Handling data:");
                    System.out.println("        x = " + x);
                    System.out.println("        y = " + y);
                    readPixels(x, y, sourcePixels);
                    for (int i = 0; i < targetPixels.length; i++) {
                        targetPixels[i] = (byte) sourcePixels[i];
                    }
                    lcclass.setData(new ProductData.Byte(targetPixels));
                    writeProduct(tileProduct);
                }

                private void writeProduct(Product tileProduct) {
                    try {
                        ProductIO.writeProduct(tileProduct, fileSystem.getPath(outputPath, String.format("lc-%s-%s.nc", year, tile)).toFile(), "NetCDF4-CF", false);
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                }

                private void readPixels(int x, int y, int[] sourcePixels) {
                    try {
                        product.getBand("lccs_class").readPixels(x, y, RASTER_LENGTH, RASTER_LENGTH, sourcePixels);
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                }
            });
        }

        ExecutorService exec = Executors.newFixedThreadPool(1);
        try {
            for (Runnable task : tasks) {
                exec.submit(task);
            }
        } finally {
            exec.shutdown();
        }
    }


    private static int getX(String tile) {
        return Integer.parseInt(tile.substring(4)) * RASTER_LENGTH;
    }

    private static int getY(String tile) {
        return Integer.parseInt(tile.substring(1, 3)) * RASTER_LENGTH;
    }


    private static List<String> readTiles() throws IOException {
        List<String> tiles = new ArrayList<>();
        Path path = FileSystems.getDefault().getPath("D:\\workspace\\fire-cci\\lc-data-to-split", "tiles.txt");
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                tiles.add(line);
            }
        }
        return tiles;
    }

}
