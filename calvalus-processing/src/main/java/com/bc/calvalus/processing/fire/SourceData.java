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
    float[] observedArea;

    SourceData() {
        pixels = new int[90 * 90];
        lcClasses = new int[90 * 90];
        areas = new double[90 * 90];
        observedArea = new float[90 * 90];
    }

    SourceData(int[] pixels, double[] areas, int[] lcClasses, float[] observedArea) {
        this.pixels = pixels;
        this.areas = areas;
        this.lcClasses = lcClasses;
        this.observedArea = observedArea;
    }

    void reset() {
        Arrays.fill(pixels, FireGridMapper.NO_DATA);
        Arrays.fill(lcClasses, 0);
        Arrays.fill(areas, FireGridMapper.NO_AREA);
        Arrays.fill(observedArea, 0);
    }

}
