package com.bc.calvalus.processing.fire.format.grid;

import java.util.Arrays;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.NO_AREA;
import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.NO_DATA;

/**
 * @author thomas
 */
public class SourceData {

    public int width;
    public int height;
    public float[] burnedPixels;
    public double[] areas;
    public int[] lcClasses;
    public boolean[] burnable;
    public int patchCount;
    public int[] statusPixels;
    public double[] probabilityOfBurn;

    public SourceData(int width, int height) {
        this.width = width;
        this.height = height;
        burnedPixels = new float[width * height];
        areas = new double[width * height];
        statusPixels = new int[width * height];
        lcClasses = new int[width * height];
        burnable = new boolean[width * height];
        probabilityOfBurn = new double[width * height];
    }

    public void reset() {
        Arrays.fill(burnedPixels, NO_DATA);
        Arrays.fill(lcClasses, 0);
        Arrays.fill(areas, NO_AREA);
        Arrays.fill(statusPixels, 0);
        Arrays.fill(burnable, false);
        Arrays.fill(probabilityOfBurn, 0.0);

    }

}
