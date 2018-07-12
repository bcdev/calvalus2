package com.bc.calvalus.processing.fire.format.grid.avhrr;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.common.reproject.ReprojectionOp;

import java.io.File;
import java.io.IOException;
import java.time.Year;
import java.util.Arrays;
import java.util.List;

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

//        File datesProductFile = CalvalusProductIO.copyFileToLocal(paths[0], context.getConfiguration());
//        Product datesProduct = reproject(ProductIO.readProduct(datesProductFile));
        File porcProductFile = CalvalusProductIO.copyFileToLocal(paths[1], context.getConfiguration());
        Product porcProduct = reproject(ProductIO.readProduct(porcProductFile));
        File uncProductFile = CalvalusProductIO.copyFileToLocal(paths[2], context.getConfiguration());
        Product uncProduct = reproject(ProductIO.readProduct(uncProductFile));

        File lcFile = CalvalusProductIO.copyFileToLocal(new Path("hdfs://calvalus/calvalus/projects/fire/aux/lc-avhrr.nc"), context.getConfiguration());
        Product lcProduct = ProductIO.readProduct(lcFile);

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
        ReprojectionOp reprojectionOp = new ReprojectionOp();
        reprojectionOp.setSourceProduct(product);
        reprojectionOp.setParameterDefaultValues();
        reprojectionOp.setParameter("crs", "EPSG:4326");
        return reprojectionOp.getTargetProduct();
    }

    @Override
    protected boolean maskUnmappablePixels() {
        return false;
    }

    @Override
    protected void validate(float burnableFraction, List<float[]> baInLc, int targetGridCellIndex, double area) {
        double lcAreaSum = 0.0F;
        for (float[] baValues : baInLc) {
            lcAreaSum += baValues[targetGridCellIndex];
        }
        float lcAreaSumFraction = getFraction(lcAreaSum, area);
        if (lcAreaSumFraction > burnableFraction * 1.05) {
            throw new IllegalStateException("fraction of burned pixels in LC classes (" + lcAreaSumFraction + ") > burnable fraction (" + burnableFraction + ") at target pixel " + targetGridCellIndex + "!");
        }
    }

    @Override
    protected float getErrorPerPixel(double[] probabilityOfBurn, double averageArea, int numberOfBurnedPixels) {
        double sum_pb = 0.0;
        for (double p : probabilityOfBurn) {
            if (p < 0) {
                throw new IllegalStateException("p < 0");
            }
            if (Double.isNaN(p)) {
                continue;
            }
            if (p > 1) {
                // no-data/cloud/water
                continue;
            }
            sum_pb += p;
        }

        double S = numberOfBurnedPixels / sum_pb;
        if (Double.isNaN(S) || Double.isInfinite(S)) {
            return 0;
        }

        double[] pb_i_star = new double[probabilityOfBurn.length];

        for (int i = 0; i < probabilityOfBurn.length; i++) {
            double pb_i = probabilityOfBurn[i];
            if (Double.isNaN(pb_i)) {
                continue;
            }
            if (pb_i > 1) {
                // no-data/cloud/water
                continue;
            }
            pb_i_star[i] = pb_i * S;
        }

        double checksum = 0.0;
        for (double v : pb_i_star) {
            checksum += v;
        }

        if (Math.abs(checksum - numberOfBurnedPixels) > 0.0005) {
            throw new IllegalArgumentException(String.format("Math.abs(checksum (%s) - numberOfBurnedPixels (%s)) > 0.0005", checksum, numberOfBurnedPixels));
        }

        double var_c = 0.0;
        int count = 0;
        for (double p : pb_i_star) {
            var_c += p * (1 - p);
//            var_c += Math.abs(p * (1 - p));
            count++;
        }

        if (count == 0) {
            return 0;
        }
        if (count == 1) {
            return 1;
        }

        return (float) Math.sqrt(var_c * (count / (count - 1.0))) * (float) averageArea;

        /*
        double[] p_b = correct(probabilityOfBurn);

        double sum_c = 0.0;
        int count = 0;
        for (double p : p_b) {
            if (Double.isNaN(p)) {
                continue;
            }
            if (p > 1) {
                // no-data/cloud/water
                continue;
            }
            if (p < 0) {
                throw new IllegalStateException("p < 0");
            }
            var_c += p * (1.0 - p);
            sum_c += p;
            count++;
        }
        if (count == 0) {
            return 0;
        }
        if (count == 1) {
            return 1;
        }

        float sqrt = (float) Math.sqrt(var_c * (count / (count - 1.0)));
        return sqrt * (float) ModisFireGridDataSource.MODIS_AREA_SIZE;

        */
    }

    @Override
    protected void predict(float[] ba, double[] areas, float[] originalErrors) {
    }

    @Override
    public boolean isValidPixel(int doyFirstOfMonth, int doyLastOfMonth, float pixel) {
        return pixel > 0.0;
    }

    @Override
    protected double scale(float burnedPixel, double area) {
        return burnedPixel * area;
    }
}