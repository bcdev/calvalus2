package com.bc.calvalus.processing.fire.format.grid.avhrr;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.CollocationOp;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
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
        LOG.info("paths=" + Arrays.toString(paths));
        if (paths.length != 4) {
            throw new IllegalStateException("Expecting dates, porcentage, uncertainty file, and tileIndex.");
        }
        int tileIndex = Integer.parseInt(paths[3].getName());

        File porcProductFile = CalvalusProductIO.copyFileToLocal(paths[1], context.getConfiguration());
        Product porcProduct = ProductIO.readProduct(porcProductFile);
        File uncProductFile = CalvalusProductIO.copyFileToLocal(paths[2], context.getConfiguration());
        Product uncProduct = ProductIO.readProduct(uncProductFile);

        String lcYear = year <= 1993 ? "1992" : year >= 2016 ? "2015" : "" + (year - 1);

        //File lcFile = CalvalusProductIO.copyFileToLocal(new Path("hdfs://calvalus/calvalus/projects/fire/aux/lc-avhrr/ESACCI-LC-L4-LCCS-Map-300m-P1Y-" + lcYear + "-v2.0.7.tif"), context.getConfiguration());
        //Product lcProduct = reproject(ProductIO.readProduct(lcFile));
        File lcFile = CalvalusProductIO.copyFileToLocal(new Path("hdfs://calvalus/calvalus/projects/fire/aux/lc-avhrr-aggregated/ESACCI-LC-L4-LCCS-Map-300m-P1Y-aggregated-0.050000Deg-" + lcYear + "-v2.0.7.nc"), context.getConfiguration());
        Product lcProduct = ProductIO.readProduct(lcFile);

        int doyFirstOfMonth = Year.of(year).atMonth(month).atDay(1).getDayOfYear();
        int doyLastOfMonth = Year.of(year).atMonth(month).atDay(Year.of(year).atMonth(month).lengthOfMonth()).getDayOfYear();

        AvhrrFireGridDataSource dataSource = new AvhrrFireGridDataSource(porcProduct, lcProduct, uncProduct, tileIndex);
        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);

        setDataSource(dataSource);
        GridCells gridCells = computeGridCells(year, month, context);

        context.write(new Text(String.format("%d-%02d-%d", year, month, tileIndex)), gridCells);
    }

    private Product reproject(Product product) {
        Product dummyCrsProduct = new Product("dummy", "dummy", 7200, 3600);
        dummyCrsProduct.addBand("dummy", "1");
        try {
            dummyCrsProduct.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 7200, 3600, -180.0, 90.0, 360.0 / 7200.0, 180.0 / 3600.0, 0.0, 0.0));
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
    protected float getErrorPerPixel(double[] probabilityOfBurn, double gridCellArea, double burnedPercentage) {
        // Mask all pixels with value of -1 in the confidence level layer (they should not be included in the analysis)
        double[] probabilityOfBurnMasked = Arrays.stream(probabilityOfBurn).filter(d -> d >= 0).toArray();

        // n is the number of 0.05º pixels in the 0.25º cell that were not masked
        int n = probabilityOfBurnMasked.length;

        // pixel area is the area of the 0.05º pixels
        double pixelArea = gridCellArea / 25.0;

        // the correction_factor is the one assigned to the “BA_[year]_[month]_percentage_WGS.tif” file, in a scale 0 to 1
        double CF = burnedPercentage;

        // pb_i = value of confidence level of pixel * correction_factor /100
        double[] pb = Arrays.stream(probabilityOfBurnMasked).map(d -> d * CF).toArray();

        // Var_c = sum (pb_i*(1-pb_i)
        double var_c = Arrays.stream(pb).map(pb_i -> (pb_i * (1.0 - pb_i))).sum();


        if (n == 1) {
            // SE = sqr(var_c) * pixel area
            return (float) (Math.sqrt(var_c) * pixelArea);
        } else {
            // SE = sqr(var_c*(n/(n-1))) * pixel area
            return (float) (Math.sqrt(var_c * (n / (n - 1.0))) * pixelArea);
        }
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

}