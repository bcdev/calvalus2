package com.bc.calvalus.processing.fire.format.grid;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.LcRemapping;
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
import java.util.stream.IntStream;

public abstract class AbstractGridMapper extends Mapper<Text, FileSplit, Text, GridCells> {

    protected static final Logger LOG = CalvalusLogger.getLogger();
    private final int targetRasterWidth;
    private final int targetRasterHeight;
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
        pm.beginTask("Computing grid cells...", targetRasterWidth * targetRasterHeight);
        if (dataSource == null) {
            throw new NullPointerException("dataSource == null");
        }
        int doyFirstOfMonth = Year.of(year).atMonth(month).atDay(1).getDayOfYear();
        int doyLastOfMonth = Year.of(year).atMonth(month).atDay(Year.of(year).atMonth(month).lengthOfMonth()).getDayOfYear();

        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);

        double[] areas = new double[targetRasterWidth * targetRasterHeight];
        float[] ba = new float[targetRasterWidth * targetRasterHeight];
        float[] coverage = new float[targetRasterWidth * targetRasterHeight];
        float[] patchNumber = new float[targetRasterWidth * targetRasterHeight];
        float[] errors = new float[targetRasterWidth * targetRasterHeight];
        float[] burnableFraction = new float[targetRasterWidth * targetRasterHeight];

        List<float[]> baInLc = new ArrayList<>();
        for (int c = 0; c < GridFormatUtils.LC_CLASSES_COUNT; c++) {
            float[] baInLcBuffer = new float[targetRasterWidth * targetRasterHeight];
            Arrays.fill(baInLcBuffer, 0);
            baInLc.add(baInLcBuffer);
        }

        int targetGridCellIndex = 0;
        for (int y = 0; y < targetRasterHeight; y++) {
            for (int x = 0; x < targetRasterWidth; x++) {
                System.gc();
                SourceData data = dataSource.readPixels(x, y);
                if (data == null) {
                    targetGridCellIndex++;
                    continue;
                }
                float baValue = 0.0F;
                float coverageValue = 0.0F;
                double burnableFractionValue = 0.0;

                int numberOfBurnedPixels = 0;

                for (int i = 0; i < data.burnedPixels.length; i++) {
                    int doy = data.burnedPixels[i];
                    if (isValidPixel(doyFirstOfMonth, doyLastOfMonth, doy)) {
                        numberOfBurnedPixels++;
                        baValue += data.areas[i];
                        boolean hasLcClass = false;
                        for (int lcClass = 0; lcClass < GridFormatUtils.LC_CLASSES_COUNT; lcClass++) {
                            boolean inLcClass = LcRemapping.isInLcClass(lcClass + 1, data.lcClasses[i]);
                            baInLc.get(lcClass)[targetGridCellIndex] += inLcClass ? data.areas[i] : 0.0;
                            if (inLcClass) {
                                hasLcClass = true;
                            }
                        }
                        if (!hasLcClass && data.lcClasses[i] != 0) {
//                            LOG.info("Pixel " + i + " in tile (" + x + "/" + y + ") with LC class " + data.lcClasses[i] + " is not remappable.");
                            if (maskUnmappablePixels()) {
                                baValue -= data.areas[i];
                            }
                        }
                    }

                    burnableFractionValue += data.burnable[i] ? data.areas[i] : 0.0;
                    coverageValue += data.statusPixels[i] == 1 && data.burnable[i] ? data.areas[i] : 0.0F;
                    areas[targetGridCellIndex] += data.areas[i];
                    validate(areas[targetGridCellIndex], targetGridCellIndex);
                }

                ba[targetGridCellIndex] = baValue;
                patchNumber[targetGridCellIndex] = data.patchCount;
                coverage[targetGridCellIndex] = getFraction(coverageValue, burnableFractionValue);
                burnableFraction[targetGridCellIndex] = getFraction(burnableFractionValue, areas[targetGridCellIndex]);
                validate(burnableFraction[targetGridCellIndex], baInLc, targetGridCellIndex, areas[targetGridCellIndex]);

                errors[targetGridCellIndex] = getErrorPerPixel(data.probabilityOfBurn, areas[targetGridCellIndex], numberOfBurnedPixels);

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

    public final GridCells computeGridCells(int year, int month) throws IOException {
        return computeGridCells(year, month, ProgressMonitor.NULL);
    }

    private void validate(float[] ba, double[] areas) {
        for (int i = 0; i < ba.length; i++) {
            if (ba[i] > areas[i] * 1.001) {
                throw new IllegalStateException("BA (" + ba[i] + ") > area (" + areas[i] + ") at pixel index " + i);
            }
        }
    }

    protected abstract float getErrorPerPixel(double[] probabilityOfBurn, double area, int numberOfBurnedPixels);

    protected abstract void predict(float[] ba, double[] areas, float[] originalErrors);

    protected abstract void validate(float burnableFraction, List<float[]> baInLc, int targetGridCellIndex, double area);

    protected abstract boolean maskUnmappablePixels();

    private static void validate(float[] ba, List<float[]> baInLc) {
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
//            throw new IllegalStateException("Math.abs(baSum - baInLcSum) > baSum * 0.05");
        }
    }

    private static void validate(float[] errors, float[] ba) {
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

    protected static float getFraction(double value, double area) {
        float fraction = (float) (value / area) >= 1.0F ? 1.0F : (float) (value / area);
        if (Float.isNaN(fraction)) {
            fraction = 0.0F;
        }
        return fraction;
    }

    public static boolean isValidPixel(int doyFirstOfMonth, int doyLastOfMonth, int pixel) {
        return pixel >= doyFirstOfMonth && pixel <= doyLastOfMonth && pixel != 999 && pixel != GridFormatUtils.NO_DATA;
    }

    public void setDataSource(FireGridDataSource dataSource) {
        this.dataSource = dataSource;
    }

}
