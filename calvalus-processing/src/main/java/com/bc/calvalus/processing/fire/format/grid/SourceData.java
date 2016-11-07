package com.bc.calvalus.processing.fire.format.grid;

import java.util.Arrays;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.NO_AREA;
import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.NO_DATA;

/**
 * @author thomas
 */
public class SourceData {

    public final int[] pixels;
    public final double[] areas;
    public final int[] lcClasses;
    public int patchCountFirstHalf;
    public int patchCountSecondHalf;
    public int[] statusPixelsFirstHalf;
    public int[] statusPixelsSecondHalf;

    public SourceData() {
        pixels = new int[90 * 90];
        lcClasses = new int[90 * 90];
        areas = new double[90 * 90];
        statusPixelsFirstHalf = new int[90 * 90];
        statusPixelsSecondHalf = new int[90 * 90];
    }

    public SourceData(int[] pixels, double[] areas, int[] lcClasses, int[] statusPixelsFirstHalf, int[] statusPixelsSecondHalf) {
        this.pixels = pixels;
        this.areas = areas;
        this.lcClasses = lcClasses;
        this.statusPixelsFirstHalf = statusPixelsFirstHalf;
        this.statusPixelsSecondHalf = statusPixelsSecondHalf;
    }

    public void reset() {
        Arrays.fill(pixels, NO_DATA);
        Arrays.fill(lcClasses, 0);
        Arrays.fill(areas, NO_AREA);
        Arrays.fill(statusPixelsFirstHalf, 0);
        Arrays.fill(statusPixelsSecondHalf, 0);
    }

}
