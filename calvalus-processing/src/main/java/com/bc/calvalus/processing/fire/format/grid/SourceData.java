package com.bc.calvalus.processing.fire.format.grid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.NO_AREA;
import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.NO_DATA;

/**
 * @author thomas
 */
public class SourceData {

    public int width;
    public int height;
    public int[] burnedPixels;
    public double[] areas;
    public int[] lcClasses;
    public boolean[] burnable;
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

        List<Integer> localBurned = new ArrayList<>();
        List<Double> localAreas = new ArrayList<>();
        List<Integer> localLcClasses = new ArrayList<>();
        List<Boolean> localBurnable = new ArrayList<>();
        List<Integer> localStatusPixelsFirstHalf = new ArrayList<>();
        List<Integer> localStatusPixelsSecondHalf = new ArrayList<>();
        List<Double> localProbabilityOfBurnFirstHalf = new ArrayList<>();
        List<Double> localProbabilityOfBurnSecondHalf = new ArrayList<>();
        for (Iterator<SourceData> sourceDataIter = allSourceData.iterator(); sourceDataIter.hasNext(); ) {
            SourceData sourceData = sourceDataIter.next();
            localBurned.add(sourceData.burnedPixels[0]);
            localAreas.add(sourceData.areas[0]);
            localLcClasses.add(sourceData.lcClasses[0]);
            localBurnable.add(sourceData.burnable[0]);
            localStatusPixelsFirstHalf.add(sourceData.statusPixelsFirstHalf[0]);
            localStatusPixelsSecondHalf.add(sourceData.statusPixelsSecondHalf[0]);
            localProbabilityOfBurnFirstHalf.add(sourceData.probabilityOfBurnFirstHalf[0]);
            localProbabilityOfBurnSecondHalf.add(sourceData.probabilityOfBurnSecondHalf[0]);
            sourceDataIter.remove();
            System.gc();
        }

        if (!allSourceData.isEmpty()) {
            throw new IllegalStateException("Implementation error, list not empty.");
        }

        SourceData result = new SourceData(localBurned.size(), 1);
        for (int i = 0; i < localBurned.size(); i++) {
            result.burnedPixels[i] = localBurned.get(i);
            result.areas[i] = localAreas.get(i);
            result.lcClasses[i] = localLcClasses.get(i);
            result.burnable[i] = localBurnable.get(i);
            result.statusPixelsFirstHalf[i] = localStatusPixelsFirstHalf.get(i);
            result.statusPixelsSecondHalf[i] = localStatusPixelsSecondHalf.get(i);
            result.probabilityOfBurnFirstHalf[i] = localProbabilityOfBurnFirstHalf.get(i);
            result.probabilityOfBurnSecondHalf[i] = localProbabilityOfBurnSecondHalf.get(i);
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
