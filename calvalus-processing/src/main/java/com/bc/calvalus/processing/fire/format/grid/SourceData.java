package com.bc.calvalus.processing.fire.format.grid;

import java.util.Arrays;
import java.util.List;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.NO_AREA;
import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.NO_DATA;

/**
 * @author thomas
 */
public class SourceData {

    public final int width;
    public final int height;
    public final int[] burnedPixels;
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
        this.width = width;
        this.height = height;
        burnedPixels = new int[width * height];
        areas = new double[width * height];
        statusPixelsFirstHalf = new int[width * height];
        statusPixelsSecondHalf = new int[width * height];
        lcClasses = new int[width * height];
        burnable = new boolean[width * height];
        probabilityOfBurnFirstHalf = new double[width * height];
        probabilityOfBurnSecondHalf = new double[width * height];
    }

    public static SourceData merge(List<SourceData> allSourceData) {
        int width = allSourceData.size();
        SourceData result = new SourceData(width, 1);
        int offset = 0;
        for (SourceData sourceData : allSourceData) {
            System.arraycopy(sourceData.burnedPixels, 0, result.burnedPixels, offset, 1);
            System.arraycopy(sourceData.lcClasses, 0, result.lcClasses, offset, 1);
            System.arraycopy(sourceData.statusPixelsFirstHalf, 0, result.statusPixelsFirstHalf, offset, 1);
            System.arraycopy(sourceData.statusPixelsSecondHalf, 0, result.statusPixelsSecondHalf, offset, 1);
            System.arraycopy(sourceData.statusPixelsSecondHalf, 0, result.statusPixelsSecondHalf, offset, 1);
            System.arraycopy(sourceData.burnable, 0, result.burnable, offset, 1);
            System.arraycopy(sourceData.probabilityOfBurnFirstHalf, 0, result.probabilityOfBurnFirstHalf, offset, 1);
            System.arraycopy(sourceData.probabilityOfBurnSecondHalf, 0, result.probabilityOfBurnSecondHalf, offset, 1);
            offset++;
        }
        return result;
    }

    public void reset() {
        Arrays.fill(burnedPixels, NO_DATA);
        Arrays.fill(lcClasses, 0);
        Arrays.fill(areas, NO_AREA);
        Arrays.fill(statusPixelsFirstHalf, 0);
        Arrays.fill(statusPixelsSecondHalf, 0);
        Arrays.fill(burnable, false);
        Arrays.fill(probabilityOfBurnFirstHalf, 0.0);
        Arrays.fill(probabilityOfBurnSecondHalf, 0.0);

    }

}
