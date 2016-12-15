package com.bc.calvalus.processing.fire.format.grid;

import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;

import java.util.ArrayList;
import java.util.List;

import static com.bc.calvalus.processing.fire.format.grid.s2.S2FireGridDataSource.STEP;

public class GridFormatUtils {

    public static final int LC_CLASSES_COUNT = 18;
    static final int NO_DATA = -1;
    static final int NO_AREA = 0;
    public static double S2_GRID_PIXELSIZE = 0.0001810432608;

    public static int[][] make2Dims(int[] pixels) {
        int length = pixels.length;
        if ((int) (Math.sqrt(length) + 0.5) * (int) (Math.sqrt(length) + 0.5) != length) {
            throw new IllegalArgumentException();
        }
        int size = (int) Math.sqrt(length);
        return make2Dims(pixels, size, size);
    }

    public static int[][] make2Dims(int[] pixels, int width, int height) {
        int[][] result = new int[width][height];
        for (int r = 0; r < height; r++) {
            System.arraycopy(pixels, r * width, result[r], 0, width);
        }
        return result;
    }

    public static Product[] filter(String tile, Product[] sourceProducts, int x, int y) {
        int tileX = Integer.parseInt(tile.substring(4));
        int tileY = Integer.parseInt(tile.substring(1, 3));
        double upperLat = 90 - tileY * STEP;
        double lowerLat = 90 - tileY * STEP - (y + 1) / 4.0;
        double leftLon = tileX * STEP - 180 + x / 4.0;
        double rightLon = tileX * STEP - 180 + (x + 1) / 4.0;

        GeoPos UL = new GeoPos(upperLat, leftLon);
        GeoPos LL = new GeoPos(lowerLat, leftLon);
        GeoPos LR = new GeoPos(lowerLat, rightLon);
        GeoPos UR = new GeoPos(upperLat, rightLon);

        List<Product> filteredProducts = new ArrayList<>();
        for (Product sourceProduct : sourceProducts) {
            PixelPos pixelPosUL = sourceProduct.getSceneGeoCoding().getPixelPos(UL, null);
            PixelPos pixelPosLL = sourceProduct.getSceneGeoCoding().getPixelPos(LL, null);
            PixelPos pixelPosLR = sourceProduct.getSceneGeoCoding().getPixelPos(LR, null);
            PixelPos pixelPosUR = sourceProduct.getSceneGeoCoding().getPixelPos(UR, null);
            if (sourceProduct.containsPixel(pixelPosUL) ||
                    sourceProduct.containsPixel(pixelPosLL) ||
                    sourceProduct.containsPixel(pixelPosLR) ||
                    sourceProduct.containsPixel(pixelPosUR)
                    ) {
                filteredProducts.add(sourceProduct);
            }
        }

        return filteredProducts.toArray(new Product[0]);
    }
}
