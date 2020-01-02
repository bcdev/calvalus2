package com.bc.calvalus.processing.fire.format.grid;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.grid.avhrr.AvhrrFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.avhrr.AvhrrGridMapper;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.ceres.core.NullProgressMonitor;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;

import java.io.File;
import java.io.IOException;
import java.time.Year;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import static com.bc.calvalus.processing.JobConfigNames.CALVALUS_DEBUG_FIRE;

public abstract class AbstractGridMapper extends Mapper<Text, FileSplit, Text, GridCells> {

    protected static final Logger LOG = CalvalusLogger.getLogger();
    protected final int targetRasterWidth;
    protected final int targetRasterHeight;
    private FireGridDataSource dataSource;

    protected AbstractGridMapper(int targetRasterWidth, int targetRasterHeight) {
        this.targetRasterWidth = targetRasterWidth;
        this.targetRasterHeight = targetRasterHeight;
    }

    protected static ProgressMonitor getPM(Context context) {
        ProgressMonitor pm;
        if (context != null) {
            pm = new ProgressSplitProgressMonitor(context);
        } else {
            pm = new NullProgressMonitor();
        }
        return pm;
    }

    /**
     * Computes the set of grid cells for this mapper.
     */
    public final GridCells computeGridCells(int year, int month, Context context) throws IOException {
        ProgressMonitor pm = getPM(context);
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

        float[][] lcFraction = new float[1 + getLcClassesCount()][];
        for (int c = 0; c < 1 + getLcClassesCount(); ++c) {
            lcFraction[c] = new float[5 * 5];
        }
        int targetGridCellIndex = 0;
        for (int y = 0; y < targetRasterHeight; y++) {
            for (int x = 0; x < targetRasterWidth; x++) {

                SourceData data = dataSource.readPixels(x, y);

                if (data == null) {
                    targetGridCellIndex++;
                    continue;
                }

//                writeDebugProduct(context, data);

                if (this instanceof AvhrrGridMapper) {
                    double avhrrBurnedPercentage = Double.NaN;

                    ((AvhrrFireGridDataSource) dataSource).readLcFraction(x, y, lcFraction);

                    double area025 = 0.0;
                    double burnableArea025 = 0.0;
                    double burnable20PercentArea025 = 0.0;
                    double observedArea025 = 0.0;
                    double burnedArea025 = 0.0;
                    double[] burnedLcArea025 = new double[19];
                    Arrays.fill(burnedLcArea025, 0.0);
                    for (int i = 0; i < data.burnedPixels.length; i++) {

                        if (Double.isNaN(avhrrBurnedPercentage) && data.burnedPixels[i] > 0) {
                            avhrrBurnedPercentage = data.burnedPixels[i];
                        }

                        double fractionOfBurnable = 1.0 - lcFraction[0][i];
                        // sum up area
                        area025 += data.areas[i];
                        // sum up burnable area (and the rather doubtful burnable areas with fraction >= 0.2)
                        burnableArea025 += data.areas[i] * fractionOfBurnable;
                        if (data.statusPixels[i] != 2) {
                            burnable20PercentArea025 += data.areas[i] * fractionOfBurnable;
                        }
                        // sum up observed burnable area
                        if (data.statusPixels[i] == 1) {
                            observedArea025 += data.areas[i] * fractionOfBurnable;
                        }
                        // sum up burned area
                        if (data.burnedPixels[i] > 0.0) {
                            burnedArea025 += data.areas[i] * data.burnedPixels[i];
                            // sum up burned area per LC class
                            for (int c = 1; c < 19; ++c) {
                                burnedLcArea025[c] += data.areas[i] * data.burnedPixels[i] * lcFraction[c][i] / fractionOfBurnable;
                            }
                        }
                    }

                    areas[targetGridCellIndex] = area025;
                    ba[targetGridCellIndex] = burnedArea025;
                    for (int c = 1; c < 19; ++c) {
                        baInLc.get(c - 1)[targetGridCellIndex] = burnedLcArea025[c];
                    }
                    burnableFraction[targetGridCellIndex] = (float) (burnableArea025 / area025);
                    // tough rather doubtful we map non-burnable to not observed as requested by UAH
                    coverage[targetGridCellIndex] = burnable20PercentArea025 > 0.0 ? (float) (observedArea025 / burnable20PercentArea025) : 0.0f;
                    patchNumber[targetGridCellIndex] = data.patchCount;

                    if (burnedArea025 >= 0.00001) {
                        errors[targetGridCellIndex] = getErrorPerPixel(data.probabilityOfBurn, area025, avhrrBurnedPercentage);
                    } else {
                        errors[targetGridCellIndex] = 0;
                    }

                } else {

                    double baValue = 0.0F;
                    double coverageValue = 0.0F;
                    double burnableFractionValue = 0.0;

                    for (int i = 0; i < data.burnedPixels.length; i++) {
                        float burnedPixel = data.burnedPixels[i];
                        boolean isBurnable = data.burnable[i];
                        if (isActuallyBurnedPixel(doyFirstOfMonth, doyLastOfMonth, burnedPixel, isBurnable)) {
                            double burnedArea = scale(burnedPixel, data.areas[i]);
                            baValue += burnedArea;
                            addBaInLandCover(baInLc, targetGridCellIndex, burnedArea, data.lcClasses[i]);
                        }

                        burnableFractionValue += isBurnable ? data.areas[i] : 0.0;
                        boolean hasBeenObserved = data.statusPixels[i] == 1;
                        coverageValue += (hasBeenObserved && isBurnable) ? data.areas[i] : 0.0;
                        areas[targetGridCellIndex] += data.areas[i];
                        validate(areas[targetGridCellIndex], targetGridCellIndex);
                    }

                    ba[targetGridCellIndex] = baValue;
                    patchNumber[targetGridCellIndex] = data.patchCount;

                    if (isInBrokenLCZone(x, y)) {
                        coverage[targetGridCellIndex] = 0;
                        burnableFraction[targetGridCellIndex] = 0;
                    } else {
                        coverage[targetGridCellIndex] = getFraction(coverageValue, burnableFractionValue);
                        burnableFraction[targetGridCellIndex] = getFraction(burnableFractionValue, areas[targetGridCellIndex]);
                        validate(burnableFraction[targetGridCellIndex], baInLc, targetGridCellIndex, areas[targetGridCellIndex]);
                    }


                    errors[targetGridCellIndex] = getErrorPerPixel(data.probabilityOfBurn, areas[targetGridCellIndex], Float.NaN);

                    for (int i = 0; i < errors.length; i++) {
                        if (ba[i] < 0.00001) {
                            errors[i] = 0;
                        }
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

    protected abstract int getLcClassesCount();

    protected abstract void addBaInLandCover(List<double[]> baInLc, int targetGridCellIndex, double burnedArea, int sourceLc);

    public final GridCells computeGridCells(int year, int month) throws IOException {
        return computeGridCells(year, month, null);
    }

    private void validate(double[] ba, double[] areas) {
        for (int i = 0; i < ba.length; i++) {
            if (ba[i] > areas[i] * 1.001) {
                throw new IllegalStateException("BA (" + ba[i] + ") > area (" + areas[i] + ") at pixel index " + i);
            }
        }
    }

    protected abstract float getErrorPerPixel(double[] probabilityOfBurn, double gridCellArea, double burnedPercentage);

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

    protected float getFraction(double value, double area) {
        if (area < 0.0001) {
            return 0.0F;
        }
        float fraction = Math.min((float) (value / area), 1.0F);
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

    private static void writeDebugProduct(Context context, SourceData data) throws IOException {
        if (context.getConfiguration().getBoolean(CALVALUS_DEBUG_FIRE, false)) {
            return;
        }
        if (data == null) {
            LOG.warning("Data is null, unable to create debug file.");
            return;
        }
        Product sourceData = data.makeProduct();
        ProductIO.writeProduct(sourceData, "./debug.nc", "NetCDF4-CF");
        Path path = new Path("hdfs://calvalus/calvalus/home/thomas/debug.nc");

        FileSystem fs = path.getFileSystem(context.getConfiguration());
        if (!fs.exists(path)) {
            FileUtil.copy(new File("./debug.nc"), fs, path, false, context.getConfiguration());
        }

        LOG.info("created debug file at hdfs://calvalus/calvalus/home/thomas/debug.nc");
    }

}
