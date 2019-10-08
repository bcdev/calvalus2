package com.bc.calvalus.processing.fire.format.grid;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.ProductData;

import java.awt.*;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.NO_DATA;

public abstract class AbstractFireGridDataSource implements FireGridDataSource {

    protected int doyFirstOfMonth = -1;
    protected int doyLastOfMonth = -1;

    private final SortedMap<String, Integer> bandToMinY;
    private final SortedMap<String, ProductData> data;

    private final int cacheSize;
    private final int rasterWidth;

    protected AbstractFireGridDataSource(int cacheSize, int rasterWidth) {
        this.cacheSize = cacheSize;
        this.rasterWidth = rasterWidth;
        data = new TreeMap<>();
        bandToMinY = new TreeMap<>();
    }

    @Override
    public void setDoyFirstOfMonth(int doyFirstOfMonth) {
        this.doyFirstOfMonth = doyFirstOfMonth;
    }

    @Override
    public void setDoyLastOfMonth(int doyLastOfMonth) {
        this.doyLastOfMonth = doyLastOfMonth;
    }

    public int getPatchNumbers(float[][] pixels, boolean[][] burnable) {
        int patchCount = 0;
        for (int i = 0; i < pixels.length; i++) {
            for (int j = 0; j < pixels[i].length; j++) {
                if (clearObjects(pixels, burnable, i, j)) {
                    patchCount++;
                }
            }
        }
        return patchCount;
    }

    public float getFloatPixelValue(Band band, String tile, int pixelIndex) throws IOException {
        String key = band.getName() + "_" + tile;
        refreshCache(band, key, pixelIndex);
        int subPixelIndex = pixelIndex % rasterWidth + ((pixelIndex / rasterWidth) % cacheSize) * rasterWidth;
        return data.get(key).getElemFloatAt(subPixelIndex);
    }

    public int getIntPixelValue(Band band, String tile, int pixelIndex) throws IOException {
        String key = band.getName() + "_" + tile;
        refreshCache(band, key, pixelIndex);
        int subPixelIndex = pixelIndex % rasterWidth + ((pixelIndex / rasterWidth) % cacheSize) * rasterWidth;
        return data.get(key).getElemIntAt(subPixelIndex);
    }

    private void refreshCache(Band band, String key, int pixelIndex) throws IOException {
        int currentMinY;
        if (bandToMinY.containsKey(key)) {
            currentMinY = bandToMinY.get(key);
        } else {
            int pixelIndexY = pixelIndex / rasterWidth;
            currentMinY = pixelIndexY - pixelIndexY % cacheSize;
        }

        int pixelIndexY = pixelIndex / rasterWidth;
        boolean pixelIndexIsInCache = pixelIndexY >= currentMinY && pixelIndexY < currentMinY + cacheSize;
        boolean alreadyRead = pixelIndexIsInCache && data.containsKey(key);
        if (!alreadyRead) {
            ProductData productData = ProductData.createInstance(band.getDataType(), band.getRasterWidth() * cacheSize);
            band.readRasterData(0, pixelIndexY - pixelIndexY % cacheSize, rasterWidth, cacheSize, productData);
            data.put(key, productData);
            currentMinY = pixelIndexY - pixelIndexY % cacheSize;
            bandToMinY.put(key, currentMinY);
        }
    }

    private boolean clearObjects(float[][] array, boolean[][] burnable, int x, int y) {
        if (x < 0 || y < 0 || x >= array.length || y >= array[x].length) {
            return false;
        }
        if (burnable[x][y] && isBurned(array[x][y])) {
            array[x][y] = 0;
            clearObjects(array, burnable, x - 1, y);
            clearObjects(array, burnable, x + 1, y);
            clearObjects(array, burnable, x, y - 1);
            clearObjects(array, burnable, x, y + 1);
            return true;
        }
        return false;
    }

    private boolean isBurned(float pixel) {
        if (doyFirstOfMonth == -1 || doyLastOfMonth == -1) {
            throw new IllegalStateException("doyFirstHalf == -1 || doySecondHalf == -1 || doyFirstOfMonth == -1 || doyLastOfMonth == -1");
        }
        return pixel >= doyFirstOfMonth && pixel <= doyLastOfMonth && pixel != 999 && pixel != NO_DATA;
    }

    protected static void setAreas(GeoCoding gc, Rectangle sourceRect, double[] areas) {
        AreaCalculator areaCalculator = new AreaCalculator(gc);
        int pixelIndex = 0;
        for (int y = sourceRect.y; y < sourceRect.y + sourceRect.height; y++) {
            for (int x = sourceRect.x; x < sourceRect.x + sourceRect.width; x++) {
                areas[pixelIndex++] = areaCalculator.calculatePixelSize(x, y);
            }
        }
    }

    protected static boolean isValidPixel(int doyFirstOfMonth, int doyLastOfMonth, int pixel) {
        return pixel >= doyFirstOfMonth && pixel <= doyLastOfMonth;
    }

    public static double getUpperLat(int y) {
        if (y < 0 || y > 719) {
            throw new IllegalArgumentException("invalid value of y: " + y + "; y has to be between 0 and 719.");
        }
        return 90 - 0.25 * y;
    }

    public static double getLeftLon(int x) {
        if (x < 0 || x > 1439) {
            throw new IllegalArgumentException("invalid value of x: " + x + "; x has to be between 0 and 1439.");
        }
        return -180 + 0.25 * x;
    }

}
