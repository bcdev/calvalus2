package com.bc.calvalus.processing.fire.format.grid;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.time.Year;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public abstract class AbstractGridMapper extends Mapper<Text, FileSplit, Text, GridCells> {

    protected static final Logger LOG = CalvalusLogger.getLogger();
    protected final int targetRasterWidth;
    protected final int targetRasterHeight;
    private FireGridDataSource dataSource;

    protected AbstractGridMapper(int targetRasterWidth, int targetRasterHeight) {
        this.targetRasterWidth = targetRasterWidth;
        this.targetRasterHeight = targetRasterHeight;
    }

    /**
     * Computes the set of grid cells for this mapper.
     */
    public final GridCells computeGridCells(int year, int month, ProgressMonitor pm) throws IOException {
        LOG.info("Computing grid cells...");
        if (dataSource == null) {
            throw new NullPointerException("dataSource == null");
        }
        int doyFirstOfMonth = Year.of(year).atMonth(month).atDay(1).getDayOfYear();
        int doyLastOfMonth = Year.of(year).atMonth(month).atDay(Year.of(year).atMonth(month).lengthOfMonth()).getDayOfYear();

        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);

        double[] areas = new double[targetRasterWidth * targetRasterHeight];
        double[] ba = new double[targetRasterWidth * targetRasterHeight];
        float[] coverage = new float[targetRasterWidth * targetRasterHeight];
        float[] patchNumber = new float[targetRasterWidth * targetRasterHeight];
        float[] errors = new float[targetRasterWidth * targetRasterHeight];
        float[] burnableFraction = new float[targetRasterWidth * targetRasterHeight];

        List<double[]> baInLc = new ArrayList<>();
        for (int c = 0; c < getLcClassesCount(); c++) {
            double[] baInLcBuffer = new double[targetRasterWidth * targetRasterHeight];
            Arrays.fill(baInLcBuffer, 0.0);
            baInLc.add(baInLcBuffer);
        }

        int targetGridCellIndex = 0;
        for (int y = 0; y < targetRasterHeight; y++) {
            for (int x = 0; x < targetRasterWidth; x++) {
                System.gc();

                if (targetGridCellIndex != 15) {
                    targetGridCellIndex++;
                    continue;
                }

                SourceData data = dataSource.readPixels(x, y);
                if (data == null) {
                    targetGridCellIndex++;
                    continue;
                }

                List<Integer> poses = new ArrayList<>();
                float[] burnedPixels = data.burnedPixels;
                for (int i = 0; i < burnedPixels.length; i++) {
                    float d = burnedPixels[i];
                    if (d > 0.0 && d < 367) {
                        poses.add(i);
                    }
                }

                double[] burnedToPrint = new double[poses.size()];
                double[] areasToPrint = new double[poses.size()];
                boolean[] burnableToPrint = new boolean[poses.size()];
                for (int i = 0; i < poses.size(); i++) {
                    Integer pos = poses.get(i);
                    burnedToPrint[i] = data.burnedPixels[pos];
                    areasToPrint[i] = data.areas[pos];
                    burnableToPrint[i] = data.burnable[pos];
                }

                System.out.println(Arrays.toString(burnedToPrint));
                System.out.println(Arrays.toString(areasToPrint));
                System.out.println(Arrays.toString(burnableToPrint));
                System.out.println(data.patchCount);

                double baValue = 0.0F;
                double coverageValue = 0.0F;
                double burnableFractionValue = 0.0;

                int numberOfBurnedPixels = 0;

                for (int i = 0; i < data.burnedPixels.length; i++) {
                    float burnedPixel = data.burnedPixels[i];
                    if (isActuallyBurnedPixel(doyFirstOfMonth, doyLastOfMonth, burnedPixel, data.burnable[i])) {
                        numberOfBurnedPixels++;
                        double burnedArea = scale(burnedPixel, data.areas[i]);
                        baValue += burnedArea;
                        addBaInLandCover(baInLc, targetGridCellIndex, burnedArea, data.lcClasses[i]);
                    }

                    burnableFractionValue += data.burnable[i] ? data.areas[i] : 0.0;
                    coverageValue += (data.statusPixels[i] == 1 && data.burnable[i]) ? data.areas[i] : 0.0;
                    areas[targetGridCellIndex] += data.areas[i];
                    validate(areas[targetGridCellIndex], targetGridCellIndex);
                }

                ba[targetGridCellIndex] = baValue;
                patchNumber[targetGridCellIndex] = data.patchCount;
                coverage[targetGridCellIndex] = getFraction(coverageValue, burnableFractionValue);
                burnableFraction[targetGridCellIndex] = getFraction(burnableFractionValue, areas[targetGridCellIndex]);
                validate(burnableFraction[targetGridCellIndex], baInLc, targetGridCellIndex, areas[targetGridCellIndex]);

                errors[targetGridCellIndex] = getErrorPerPixel(data.probabilityOfBurn, areas[targetGridCellIndex], numberOfBurnedPixels, baValue);

                for (int i = 0; i < errors.length; i++) {
                    if (ba[i] < 0.00001) {
                        errors[i] = 0;
                    }
                }

                targetGridCellIndex++;
                pm.worked(1);

            }
        }

        predict(ba, areas, errors);
        validate(errors, ba);
        validate(ba, baInLc);
        validate(ba, areas);

        GridCells gridCells = new GridCells();
        gridCells.lcClassesCount = getLcClassesCount();
        gridCells.bandSize = targetRasterWidth * targetRasterHeight;
        gridCells.setBa(ba);
        gridCells.setPatchNumber(patchNumber);
        gridCells.setErrors(errors);
        gridCells.setBaInLc(baInLc);
        gridCells.setCoverage(coverage);
        gridCells.setBurnableFraction(burnableFraction);
        LOG.info("...done.");
        pm.done();
        return gridCells;
    }

    protected abstract int getLcClassesCount();

    protected abstract void addBaInLandCover(List<double[]> baInLc, int targetGridCellIndex, double burnedArea, int sourceLc);

    public final GridCells computeGridCells(int year, int month) throws IOException {
        return computeGridCells(year, month, ProgressMonitor.NULL);
    }

    private void validate(double[] ba, double[] areas) {
        for (int i = 0; i < ba.length; i++) {
            if (ba[i] > areas[i] * 1.001) {
                throw new IllegalStateException("BA (" + ba[i] + ") > area (" + areas[i] + ") at pixel index " + i);
            }
        }
    }

    protected abstract float getErrorPerPixel(double[] probabilityOfBurn, double area, int numberOfBurnedPixels, double burnedArea);

    protected abstract void predict(double[] ba, double[] areas, float[] originalErrors);

    protected abstract void validate(float burnableFraction, List<double[]> baInLc, int targetGridCellIndex, double area);

    private static void validate(double[] ba, List<double[]> baInLc) {
        for (int i = 0; i < ba.length; i++) {
            double v = ba[i];
            double baInLcSum = 0.0F;
            for (double[] floats : baInLc) {
                baInLcSum += floats[i];
            }
            if (Math.abs(v - baInLcSum) > 0.05 * v) {
                CalvalusLogger.getLogger().warning("Math.abs(baSum - baInLcSum) > baSum * 0.05:");
                CalvalusLogger.getLogger().warning("baSum = " + v);
                CalvalusLogger.getLogger().warning("baInLcSum " + baInLcSum);
                CalvalusLogger.getLogger().warning("targetGridCellIndex " + i);
                throw new IllegalStateException("Math.abs(baSum - baInLcSum) > baSum * 0.05");
            }
        }

        /*

        int baCount = (int) IntStream.range(0, ba.length).mapToDouble(i -> ba[i]).filter(i -> i != 0).count();
        if (baCount < ba.length * 0.05) {
            // don't throw an error for too few observations
            return;
        }

        float baSum = (float) IntStream.range(0, ba.length).mapToDouble(i -> ba[i]).sum();
        float baInLcSum = 0;
        for (float[] floats : baInLc) {
            baInLcSum += IntStream.range(0, floats.length).mapToDouble(i -> floats[i]).sum();
        }
        if (Math.abs(baSum - baInLcSum) > baSum * 0.05) {
            CalvalusLogger.getLogger().warning("Math.abs(baSum - baInLcSum) > baSum * 0.05:");
            CalvalusLogger.getLogger().warning("baSum = " + baSum);
            CalvalusLogger.getLogger().warning("baInLcSum " + baInLcSum);
            throw new IllegalStateException("Math.abs(baSum - baInLcSum) > baSum * 0.05");
        }

        */
    }

    private static void validate(float[] errors, double[] ba) {
        for (int i = 0; i < errors.length; i++) {
            float error = errors[i];
            // todo - check!
            if (error > 0 && !(ba[i] > 0)) {
                LOG.warning("error > 0 && !(ba[i] > 0)");
                throw new IllegalStateException("error > 0 && !(ba[i] > 0)");
            }
            if (Float.isNaN(error)) {
                LOG.warning("error is NaN");
//                throw new IllegalStateException("error is NaN");
            }
        }
    }

    private static void validate(double area, int index) {
        if (area < 0) {
            throw new IllegalStateException("area < 0 at target pixel " + index);
        }
    }

    public static float getFraction(double value, double area) {
        float fraction = (float) (value / area) >= 1.0F ? 1.0F : (float) (value / area);
        if (Float.isNaN(fraction)) {
            fraction = 0.0F;
        }
        return fraction;
    }

    protected double scale(float burnedPixel, double area) {
        return area;
    }

    public boolean isActuallyBurnedPixel(int doyFirstOfMonth, int doyLastOfMonth, float pixel, boolean isBurnable) {
        return (pixel >= doyFirstOfMonth && pixel <= doyLastOfMonth && pixel != 999 && pixel != GridFormatUtils.NO_DATA) && isBurnable;
    }

    public void setDataSource(FireGridDataSource dataSource) {
        this.dataSource = dataSource;
    }

}