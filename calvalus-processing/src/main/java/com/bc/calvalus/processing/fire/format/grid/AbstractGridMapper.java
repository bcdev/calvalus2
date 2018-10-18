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

                SourceData data = dataSource.readPixels(x, y);
                if (data == null) {
                    targetGridCellIndex++;
                    continue;
                }

                double baValue = 0.0F;
                double coverageValue = 0.0F;
                double specialCoverageValue = 0.0F;
                double burnableFractionValue = 0.0;

                int numberOfBurnedPixels = 0;

                for (int i = 0; i < data.burnedPixels.length; i++) {
                    float burnedPixel = data.burnedPixels[i];
                    boolean isBurnable = data.burnable[i];
                    if (isActuallyBurnedPixel(doyFirstOfMonth, doyLastOfMonth, burnedPixel, isBurnable)) {
                        numberOfBurnedPixels++;
                        double burnedArea = scale(burnedPixel, data.areas[i]);
                        baValue += burnedArea;
                        addBaInLandCover(baInLc, targetGridCellIndex, burnedArea, data.lcClasses[i]);
                    }

                    burnableFractionValue += isBurnable ? data.areas[i] : 0.0;
                    boolean hasBeenObserved = data.statusPixels[i] == 1;
                    coverageValue += (hasBeenObserved && isBurnable) ? data.areas[i] : 0.0;
                    specialCoverageValue += hasBeenObserved ? data.areas[i] : 0.0;
                    areas[targetGridCellIndex] += data.areas[i];
                    validate(areas[targetGridCellIndex], targetGridCellIndex);
                }

                ba[targetGridCellIndex] = baValue;
                patchNumber[targetGridCellIndex] = data.patchCount;

                if (mustHandleCoverageSpecifially(x)) {
                    LOG.info("Handling LC specially.");
                    LOG.info("specialCoverageValue=" + specialCoverageValue);
                    double[] areaAndSpecialBurnableFractionValue = getAreaAndSpecialBurnableFractionValue(x, y);
                    areas[targetGridCellIndex] = areaAndSpecialBurnableFractionValue[0];
                    burnableFractionValue = areaAndSpecialBurnableFractionValue[1];

                    LOG.info("areas=" + areas[targetGridCellIndex]);
                    LOG.info("burnableFractionValue=" + burnableFractionValue);

                    coverageValue = specialCoverageValue;
                }

                if (isInBrokenLCZone(x, y)) {
                    coverage[targetGridCellIndex] = 0;
                    burnableFraction[targetGridCellIndex] = 0;
                } else {
                    coverage[targetGridCellIndex] = getFraction(coverageValue, burnableFractionValue);
                    burnableFraction[targetGridCellIndex] = getFraction(burnableFractionValue, areas[targetGridCellIndex]);
                    validate(burnableFraction[targetGridCellIndex], baInLc, targetGridCellIndex, areas[targetGridCellIndex]);
                }


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

    protected boolean isInBrokenLCZone(int x, int y) {
        return false;
    }

    protected double[] getAreaAndSpecialBurnableFractionValue(int x, int y) throws IOException {
        throw new IllegalStateException("This must not be called.");
    }

    protected boolean mustHandleCoverageSpecifially(int x) {
        return false;
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

    public float getFraction(double value, double area) {
        if (area < 0.0001) {
            return 0.0F;
        }
        float fraction = (float) (value / area) >= 1.0F ? 1.0F : (float) (value / area);
        if (Float.isNaN(fraction) || area == 0.0) {
            return 0.0F;
        }
        return fraction;
    }

    protected double scale(float burnedPixel, double area) {
        return area;
    }

    public boolean isActuallyBurnedPixel(int doyFirstOfMonth, int doyLastOfMonth, float pixel, boolean isBurnable) {
        return (pixel >= doyFirstOfMonth && pixel <= doyLastOfMonth) && isBurnable;
    }

    public void setDataSource(FireGridDataSource dataSource) {
        this.dataSource = dataSource;
    }

}
