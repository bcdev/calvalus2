package com.bc.calvalus.processing.fire.format.grid;

import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;

import java.util.ArrayList;
import java.util.List;

public class GridFormatUtils {

    static final int NO_DATA = -1;
    public static final int NO_AREA = 0;
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
        int[][] result = new int[height][width];
        for (int r = 0; r < height; r++) {
            System.arraycopy(pixels, r * width, result[r], 0, width);
        }
        return result;
    }

    public static float[][] make2Dims(float[] pixels) {
        int length = pixels.length;
        if ((int) (Math.sqrt(length) + 0.5) * (int) (Math.sqrt(length) + 0.5) != length) {
            throw new IllegalArgumentException();
        }
        int size = (int) Math.sqrt(length);
        return make2Dims(pixels, size, size);
    }

    public static float[][] make2Dims(float[] pixels, int width, int height) {
        float[][] result = new float[height][width];
        for (int r = 0; r < height; r++) {
            System.arraycopy(pixels, r * width, result[r], 0, width);
        }
        return result;
    }

    public static boolean[][] make2Dims(boolean[] pixels) {
        int length = pixels.length;
        if ((int) (Math.sqrt(length) + 0.5) * (int) (Math.sqrt(length) + 0.5) != length) {
            throw new IllegalArgumentException();
        }
        int size = (int) Math.sqrt(length);
        return make2Dims(pixels, size, size);
    }

    public static boolean[][] make2Dims(boolean[] pixels, int width, int height) {
        boolean[][] result = new boolean[height][width];
        for (int r = 0; r < height; r++) {
            System.arraycopy(pixels, r * width, result[r], 0, width);
        }
        return result;
    }

    public static List<Integer> getMatchingProductIndices(String tile, Product[] sourceProducts, int x, int y) {
        if (x > 7 || y > 7 || x < 0 || y < 0) {
            throw new IllegalArgumentException("x > 7 || y > 7 || x < 0 || y < 0: x=" + x + ", y=" + y);
        }
        /*
            returns only those products which overlap the target grid cell (given by x y)
            within the tile.
         */
        int tileX = Integer.parseInt(tile.split("y")[0].substring(1));
        int tileY = Integer.parseInt(tile.split("y")[1]);

        double leftLon = -180 + tileX + x * 0.25;
        double rightLon = -180 + tileX + (x + 1) * 0.25;

        double upperLat = tileY - 90 - y * 0.25;
        double lowerLat = tileY - 90 - (y + 1) * 0.25;

        GeoPos UL = new GeoPos(upperLat, leftLon);
        GeoPos LL = new GeoPos(lowerLat, leftLon);
        GeoPos LR = new GeoPos(lowerLat, rightLon);
        GeoPos UR = new GeoPos(upperLat, rightLon);

        List<Integer> indices = new ArrayList<>();

        for (int i = 0; i < sourceProducts.length; i++) {
            Product sourceProduct = sourceProducts[i];
            PixelPos pixelPosUL = sourceProduct.getSceneGeoCoding().getPixelPos(UL, null);
            PixelPos pixelPosLL = sourceProduct.getSceneGeoCoding().getPixelPos(LL, null);
            PixelPos pixelPosLR = sourceProduct.getSceneGeoCoding().getPixelPos(LR, null);
            PixelPos pixelPosUR = sourceProduct.getSceneGeoCoding().getPixelPos(UR, null);
            if (sourceProduct.containsPixel(pixelPosUL) ||
                    sourceProduct.containsPixel(pixelPosLL) ||
                    sourceProduct.containsPixel(pixelPosLR) ||
                    sourceProduct.containsPixel(pixelPosUR)
            ) {
                indices.add(i);
            }
        }
        return indices;
    }

    public static String lcYear(int year) {
        // 2000 - 2007 -> 2000
        // 2008 - 2012 -> 2005
        // 2013 - 2016 -> 2010
        switch (year) {
            case 2000:
            case 2001:
            case 2002:
            case 2003:
            case 2004:
            case 2005:
            case 2006:
            case 2007:
                return "2000";
            case 2008:
            case 2009:
            case 2010:
            case 2011:
            case 2012:
                return "2005";
            case 2013:
            case 2014:
            case 2015:
            case 2016:
                return "2010";
        }
        throw new IllegalArgumentException("Illegal year: " + year);
    }

    public static String modisLcYear(int year) {
        if (year == 2000) {
            return "2000";
        }
        if (year >= 2016) {
            return "2015";
        }
        return "" + (year - 1);
    }
}
