package com.bc.calvalus.processing.fire.format.grid;

import java.util.Arrays;

/**
 * @author thomas
 */
class SourceData {

    final int[] pixels;
    final double[] areas;
    final int[] lcClasses;
    int patchCountFirstHalf;
    int patchCountSecondHalf;
    int[] statusPixelsFirstHalf;
    int[] statusPixelsSecondHalf;

    SourceData() {
        pixels = new int[90 * 90];
        lcClasses = new int[90 * 90];
        areas = new double[90 * 90];
        statusPixelsFirstHalf = new int[90 * 90];
        statusPixelsSecondHalf = new int[90 * 90];
    }

    SourceData(int[] pixels, double[] areas, int[] lcClasses, int[] statusPixelsFirstHalf, int[] statusPixelsSecondHalf) {
        this.pixels = pixels;
        this.areas = areas;
        this.lcClasses = lcClasses;
        this.statusPixelsFirstHalf = statusPixelsFirstHalf;
        this.statusPixelsSecondHalf = statusPixelsSecondHalf;
    }

    void reset() {
        Arrays.fill(pixels, GridMapper.NO_DATA);
        Arrays.fill(lcClasses, 0);
        Arrays.fill(areas, GridMapper.NO_AREA);
        Arrays.fill(statusPixelsFirstHalf, 0);
        Arrays.fill(statusPixelsSecondHalf, 0);
    }

}
