package com.bc.calvalus.processing.fire;

import java.util.Arrays;

/**
 * @author thomas
 */
class SourceData {

    final int[] pixels;
    final double[] areas;
    int patchCountFirstHalf;
    int patchCountSecondHalf;

    SourceData() {
        pixels = new int[90 * 90];
        areas = new double[pixels.length];
    }

    SourceData(int[] pixels, double[] areas) {
        this.pixels = pixels;
        this.areas = areas;
    }

    void reset() {
        Arrays.fill(pixels, FireGridMapper.NO_DATA);
        Arrays.fill(areas, FireGridMapper.NO_AREA);
    }

}
