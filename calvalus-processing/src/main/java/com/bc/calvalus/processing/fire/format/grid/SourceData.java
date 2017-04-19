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
    public final boolean[] burnable;
    public int patchCountFirstHalf;
    public int patchCountSecondHalf;
    public int[] statusPixelsFirstHalf;
    public int[] statusPixelsSecondHalf;
    public double[] probabilityOfBurnFirstHalf;
    public double[] probabilityOfBurnSecondHalf;

    public SourceData(int width, int height) {
        pixels = new int[width * height];
        areas = new double[width * height];
        statusPixelsFirstHalf = new int[width * height];
        statusPixelsSecondHalf = new int[width * height];
        lcClasses = new int[width * height];
        burnable = new boolean[width * height];
        probabilityOfBurnFirstHalf = new double[width * height];
        probabilityOfBurnSecondHalf = new double[width * height];
    }

    public void reset() {
        Arrays.fill(pixels, NO_DATA);
        Arrays.fill(lcClasses, 0);
        Arrays.fill(areas, NO_AREA);
        Arrays.fill(statusPixelsFirstHalf, 0);
        Arrays.fill(statusPixelsSecondHalf, 0);
        Arrays.fill(burnable, false);
        Arrays.fill(probabilityOfBurnFirstHalf, 0.0);
        Arrays.fill(probabilityOfBurnSecondHalf, 0.0);

    }

}
