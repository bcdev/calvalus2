package com.bc.calvalus.processing.fire;

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

    SourceData() {
        pixels = new int[90 * 90];
        lcClasses = new int[90 * 90];
        areas = new double[pixels.length];
    }

    SourceData(int[] pixels, double[] areas, int[] lcClasses) {
        this.pixels = pixels;
        this.areas = areas;
        this.lcClasses = lcClasses;
    }

    void reset() {
        Arrays.fill(pixels, FireGridMapper.NO_DATA);
        Arrays.fill(lcClasses, 0);
        Arrays.fill(areas, FireGridMapper.NO_AREA);
    }

}
