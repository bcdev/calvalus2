package com.bc.calvalus.processing.fire.format.grid;

import org.esa.snap.core.datamodel.GeoCoding;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.NO_DATA;

public abstract class AbstractFireGridDataSource implements FireGridDataSource {

    protected int doyFirstOfMonth = -1;
    protected int doyLastOfMonth = -1;
    protected int doyFirstHalf = -1;
    protected int doySecondHalf = -1;

    @Override
    public void setDoyFirstOfMonth(int doyFirstOfMonth) {
        this.doyFirstOfMonth = doyFirstOfMonth;
    }

    @Override
    public void setDoyLastOfMonth(int doyLastOfMonth) {
        this.doyLastOfMonth = doyLastOfMonth;
    }

    @Override
    public void setDoyFirstHalf(int doyFirstHalf) {
        this.doyFirstHalf = doyFirstHalf;
    }

    @Override
    public void setDoySecondHalf(int doySecondHalf) {
        this.doySecondHalf = doySecondHalf;
    }

    public int getPatchNumbers(int[][] pixels, boolean firstHalf) {
        int patchCount = 0;
        for (int i = 0; i < pixels.length; i++) {
            for (int j = 0; j < pixels[i].length; j++) {
                if (clearObjects(pixels, i, j, firstHalf)) {
                    patchCount++;
                }
            }
        }
        return patchCount;
    }

    private boolean clearObjects(int[][] array, int x, int y, boolean firstHalf) {
        if (x < 0 || y < 0 || x >= array.length || y >= array[x].length) {
            return false;
        }
        if (isBurned(array[x][y], firstHalf)) {
            array[x][y] = 0;
            clearObjects(array, x - 1, y, firstHalf);
            clearObjects(array, x + 1, y, firstHalf);
            clearObjects(array, x, y - 1, firstHalf);
            clearObjects(array, x, y + 1, firstHalf);
            return true;
        }
        return false;
    }

    private boolean isBurned(int pixel, boolean firstHalf) {
        if (doyFirstHalf == -1 || doySecondHalf == -1 || doyFirstOfMonth == -1 || doyLastOfMonth == -1) {
            throw new IllegalStateException("doyFirstHalf == -1 || doySecondHalf == -1 || doyFirstOfMonth == -1 || doyLastOfMonth == -1");
        }
        if (firstHalf) {
            boolean b = pixel >= doyFirstOfMonth && pixel < doySecondHalf - 6 && pixel != 999 && pixel != NO_DATA;
            return b;
        }
        return pixel > doyFirstHalf + 8 && pixel <= doyLastOfMonth && pixel != 999 && pixel != NO_DATA;
    }

    protected static double[] getAreas(GeoCoding gc, int width, int height, double[] areas) {
        AreaCalculator areaCalculator = new AreaCalculator(gc);
        int pixelIndex = 0;
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                areas[pixelIndex++] = areaCalculator.calculatePixelSize(x, y);
            }
        }
        return areas;
    }

}
