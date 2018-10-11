package com.bc.calvalus.processing.fire.format.grid.avhrr;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.File;
import java.io.IOException;
import java.time.Year;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs the fire AVHRR formatting grid mapper.
 *
 * @author thomas
 */
public class AvhrrGridMapper extends AbstractGridMapper {

    protected AvhrrGridMapper() {
        super(80, 80);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        int year = Integer.parseInt(context.getConfiguration().get("calvalus.year"));
        int month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        Path[] paths = inputSplit.getPaths();
        if (paths.length != 4) {
            throw new IllegalStateException("Expecting dates, porcentage, uncertainty file, and tileIndex.");
        }
        LOG.info("paths=" + Arrays.toString(paths));
        int tileIndex = Integer.parseInt(paths[3].getName());

        File porcProductFile = CalvalusProductIO.copyFileToLocal(paths[1], context.getConfiguration());
        Product porcProduct = ProductIO.readProduct(porcProductFile);
        File uncProductFile = CalvalusProductIO.copyFileToLocal(paths[2], context.getConfiguration());
        Product uncProduct = ProductIO.readProduct(uncProductFile);

        String lcYear = year <= 1993 ? "1992" : year >= 2016 ? "2015" : "" + (year - 1);

        File lcFile = CalvalusProductIO.copyFileToLocal(new Path("hdfs://calvalus/calvalus/projects/fire/aux/lc-avhrr/ESACCI-LC-L4-LCCS-Map-300m-P1Y-" + lcYear + "-v2.0.7.tif"), context.getConfiguration());
        Product lcProduct = reproject(ProductIO.readProduct(lcFile));

        int doyFirstOfMonth = Year.of(year).atMonth(month).atDay(1).getDayOfYear();
        int doyLastOfMonth = Year.of(year).atMonth(month).atDay(Year.of(year).atMonth(month).lengthOfMonth()).getDayOfYear();

        AvhrrFireGridDataSource dataSource = new AvhrrFireGridDataSource(porcProduct, lcProduct, uncProduct, tileIndex);
        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);

        setDataSource(dataSource);
        GridCells gridCells = computeGridCells(year, month, new ProgressSplitProgressMonitor(context));

        context.write(new Text(String.format("%d-%02d-%d", year, month, tileIndex)), gridCells);
    }

    private Product reproject(Product product) {
        Product dummyCrsProduct = new Product("dummy", "dummy", 7200, 3600);
        dummyCrsProduct.addBand("dummy", "1");
        try {
            dummyCrsProduct.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 7200, 3600, -180.0, 90.0, 360.0 / 7200.0, 180.0 / 3600.0));
        } catch (FactoryException | TransformException e) {
            throw new IllegalStateException("Programming error, see nested exception", e);
        }
        CollocationOp collocateOp = new CollocationOp();
        collocateOp.setMasterProduct(dummyCrsProduct);
        collocateOp.setSlaveProduct(product);
        collocateOp.setParameterDefaultValues();

        Product reprojectedProduct = collocateOp.getTargetProduct();
        reprojectedProduct.removeBand(reprojectedProduct.getBand("dummy_M"));
        reprojectedProduct.getBand("band_1_S").setName("band_1");
        return reprojectedProduct;
    }

    @Override
    protected void validate(float burnableFraction, List<double[]> baInLc, int targetGridCellIndex, double area) {
        double lcAreaSum = 0.0F;
        for (double[] baValues : baInLc) {
            lcAreaSum += baValues[targetGridCellIndex];
        }
        float lcAreaSumFraction = getFraction(lcAreaSum, area);
        if (lcAreaSumFraction > burnableFraction * 1.05) {
            throw new IllegalStateException("fraction of burned pixels in LC classes (" + lcAreaSumFraction + ") > burnable fraction (" + burnableFraction + ") at target pixel " + targetGridCellIndex + "!");
        }
    }

    @Override
    protected int getLcClassesCount() {
        return LcRemapping.LC_CLASSES_COUNT;
    }

    @Override
    protected void addBaInLandCover(List<double[]> baInLc, int targetGridCellIndex, double burnedArea, int sourceLc) {
        for (int currentLcClass = 0; currentLcClass < getLcClassesCount(); currentLcClass++) {
            boolean inLcClass = LcRemapping.isInLcClass(currentLcClass + 1, sourceLc);
            baInLc.get(currentLcClass)[targetGridCellIndex] += inLcClass ? burnedArea : 0.0F;
        }
    }

    @Override
    protected float getErrorPerPixel(double[] probabilityOfBurn, double area, int numberOfBurnedPixels, double burnedArea) {
        /*
        1. Extract the number of pixels of 0.05deg that are classified as burned in the grid cell.
            It should be just the count of the pixels with value 1 to 366 inside the grid cell.
            N = count(pixels with JD>0)
        */

        int N = numberOfBurnedPixels;
        if (N == 0) {
            return 0.0F;
        }
        AtomicReference<Double> n = new AtomicReference<>(0.0);

        /*
        2.  Calculate the probability of burn of each pixel as:
            pb_i = value of confidence level of pixel  / 100
            (because the confidence level is expressed as %, to transform it to a 0-1 range)
         */

        // TS: no, the confidence value actually is in a 0-1 range, so I skip this step.

        /*
        3.  Extract the value of CF: as the CF value will be the same in all the pixels within the same grid cell and
            month, you can just extract the value of 1 pixel, and that will be enough.
            CF = correction factor of each pixel, indicating the proportion of the area of the pixel actually burned.
            This value is included in each pixel that is burned, but it is the same for each grid cell and month.
            It is in the range 0 – 1.
         */

        // TS: no formula, but I assume: CF = area / burned_area

        double CF = area / burnedArea;

        /*
        4.  sum_pb: pb_i.sum()*CF
            (this must be the sum of all the probabilities of all the pixels. Use ALL probability values inside the grid
             cell, not only the ones corresponding to the pixels that have been burned. I am multiplying CF in this step
             instead of dividing it in each pixel, as José suggested. Mathematically it is the same, and in this case the
             value of S will already include the CF correction).
         */

        double sum_pb = Arrays.stream(probabilityOfBurn).filter(v -> v >= 0).sum();

        sum_pb = sum_pb * CF;

        /*
        5.  Calculate S:
            S = N / sum_pb
         */

        double S = N / sum_pb;

        /*
        6.  Then for each pixel calculate the corrected probability:
            pb_i’= pb_i * S   (remember that CF is already applied here)
         */

        double[] pb_star = new double[(int) Arrays.stream(probabilityOfBurn).filter(d -> d >= 0).count()];
        final AtomicReference<Integer> targetIndex = new AtomicReference<>(0);
        Arrays.stream(probabilityOfBurn)
                .filter(pb_i -> pb_i >= 0)
                .forEach(
                        pb_i -> {
                            if (pb_i == 0) {
                                pb_i = 0.000000000000001;
                            }
                            pb_star[targetIndex.get()] = pb_i * S;
                            n.set((n.get() + 1));
                            targetIndex.set(targetIndex.get() + 1);
                        }
                );

        /*

        7.  Verification step: sum(pb_i’) = number_of_burn_pixels/CF .
         */

        double sum_pb_star = Arrays.stream(pb_star).sum();

        if (Math.abs(sum_pb_star - numberOfBurnedPixels / CF) > 1E5) {
            LOG.info("" + N);
            LOG.info("" + CF);
            LOG.info("" + sum_pb);
            LOG.info("" + S);
            LOG.info("" + sum_pb_star);

            throw new IllegalStateException("Error in standard error computation");
        }

        /*
        8.  For each grid cell:
            a. var() = (pb_i’ (1-pb_i’)).sum()
            b. standard_error() = sqrt(var(c) *(n/(n-1)) * A
         */

        double var_c = Arrays.stream(pb_star)
                .map(pb_star_i -> pb_star_i * (1 - pb_star_i))
                .sum();

        double A = area;

        float returnValue = (float) (Math.sqrt(var_c * (n.get() / (n.get() - 1))) * A);

        if ((burnedArea > 0 && Math.abs(returnValue) < 0) || Float.isNaN(returnValue)) {
            LOG.info("" + N);
            LOG.info("" + CF);
            LOG.info("" + sum_pb);
            LOG.info("" + S);
            LOG.info("" + sum_pb_star);
            LOG.info("" + var_c);
            LOG.info("" + A);
            LOG.info("" + n.get());
            LOG.info(Arrays.toString(probabilityOfBurn));
            LOG.info(Arrays.toString(pb_star));
            throw new IllegalStateException("Suspicious error value: " + returnValue);
        }

        return returnValue;
    }

    @Override
    protected void predict(double[] ba, double[] areas, float[] originalErrors) {
    }

    @Override
    public boolean isActuallyBurnedPixel(int doyFirstOfMonth, int doyLastOfMonth, float pixel, boolean isBurnable) {
        return pixel > 0.0 && isBurnable;
    }

    @Override
    protected double scale(float burnedPixel, double area) {
        return burnedPixel * area;
    }

    @Override
    protected boolean isBurnable(int lcClass) {
        return LcRemapping.isInBurnableLcClass(lcClass);
    }
}