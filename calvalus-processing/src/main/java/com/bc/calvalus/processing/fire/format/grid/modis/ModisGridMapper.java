package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.CommonUtils;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.util.ProductUtils;

import java.io.File;
import java.io.IOException;
import java.time.Year;
import java.util.Arrays;
import java.util.List;

/**
 * Runs the fire MODIS formatting grid mapper.
 *
 * @author thomas
 */
public class ModisGridMapper extends AbstractGridMapper {

    public static final int WINDOW_SIZE = 32;
    private String targetCell;
    private Configuration configuration;

    public ModisGridMapper() {
        super(WINDOW_SIZE, WINDOW_SIZE);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        configuration = context.getConfiguration();
        int year = Integer.parseInt(configuration.get("calvalus.year"));
        int month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        Path[] paths = inputSplit.getPaths();

        LOG.info("paths=" + Arrays.toString(paths));
        targetCell = paths[paths.length - 1].getName();
        LOG.info("targetCell=" + targetCell);

        if (paths.length == 1) {
            LOG.info("no input paths, mapper done");
            return;
        }

        int numProducts = paths.length - 1;

        Product[] sourceProducts = new Product[numProducts];
        Product[] lcProducts = new Product[numProducts];

        ProgressSplitProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        pm.beginTask("Copying data and computing grid cells...", targetRasterWidth * targetRasterHeight + paths.length - 1);

        String lcYear = GridFormatUtils.modisLcYear(year);
        int productIndex = 0;
        for (int i = 0; i < paths.length - 1; i++) {
            if (paths[i].getName().contains("dummy")) {
                Product product = new Product(paths[i].getName(), "dummy", 4800, 4800);
                product.addBand("classification", "0", ProductData.TYPE_INT16);
                product.addBand("numObs1", "3", ProductData.TYPE_UINT8);
                String tile = paths[i].getName().split("dummyburned_year_month_")[1];
                File geoCodingFile = CalvalusProductIO.copyFileToLocal(new Path("hdfs://calvalus/calvalus/projects/fire/aux/geolookup-refs/" + tile + ".hdf"), context.getConfiguration());
                Product geoCodingProduct = ProductIO.readProduct(geoCodingFile);
                ProductUtils.copyGeoCoding(geoCodingProduct, product);
                sourceProducts[productIndex] = product;
                File lcFile = CalvalusProductIO.copyFileToLocal(new Path("hdfs://calvalus/calvalus/projects/fire/aux/modis-lc/" + tile + "-" + lcYear + ".nc"), context.getConfiguration());
                lcProducts[productIndex] = ProductIO.readProduct(lcFile);
            } else {
                File sourceProductFile = CalvalusProductIO.copyFileToLocal(paths[i], context.getConfiguration());
                Product p = ProductIO.readProduct(sourceProductFile);
                if (p == null) {
                    throw new IllegalStateException("Cannot read file " + paths[i]);
                }

                String tile = paths[i].getName().split("_")[3].replace(".nc", "");
                File geoCodingFile = CalvalusProductIO.copyFileToLocal(new Path("hdfs://calvalus/calvalus/projects/fire/aux/geolookup-refs/" + tile + ".hdf"), context.getConfiguration());
                Product geoCodingProduct = ProductIO.readProduct(geoCodingFile);
                ProductUtils.copyGeoCoding(geoCodingProduct, p);
                if (p.getName().contains("h18")) {
                    Product temp = new Product(p.getName(), p.getProductType(), p.getSceneRasterWidth(), p.getSceneRasterHeight());
                    ProductUtils.copyGeoCoding(p, temp);
                    CommonUtils.fixH18Band(p, temp, "classification");
                    CommonUtils.fixH18BandByte(p, temp, "numObs1");
                    if (p.containsBand("uncertainty")) {
                        CommonUtils.fixH18BandUInt8(p, temp, "uncertainty");
                    }
                    p = temp;
                }
                sourceProducts[productIndex] = p;
                File lcFile = CalvalusProductIO.copyFileToLocal(new Path("hdfs://calvalus/calvalus/projects/fire/aux/modis-lc/" + tile + "-" + lcYear + ".nc"), context.getConfiguration());
                lcProducts[productIndex] = ProductIO.readProduct(lcFile);
            }

            productIndex++;
            pm.worked(1);
        }

        int doyFirstOfMonth = Year.of(year).atMonth(month).atDay(1).getDayOfYear();
        int doyLastOfMonth = Year.of(year).atMonth(month).atDay(Year.of(year).atMonth(month).lengthOfMonth()).getDayOfYear();

        ModisFireGridDataSource dataSource = new ModisFireGridDataSource(sourceProducts, lcProducts, targetCell);
        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);

        setDataSource(dataSource);
        GridCells gridCells = computeGridCells(year, month, context);

        context.write(new Text(String.format("%d-%02d-%s", year, month, targetCell)), gridCells);
    }

    @Override
    protected void validate(float burnableAreaFraction, List<double[]> baInLc, int targetGridCellIndex, double area) {
        double lcAreaSum = 0.0F;
        for (double[] baValues : baInLc) {
            lcAreaSum += baValues[targetGridCellIndex];
        }
        float lcAreaSumFraction = getFraction(lcAreaSum, area);
        if (lcAreaSumFraction > burnableAreaFraction * 1.2) {
            throw new IllegalStateException("lcAreaSumFraction (" + lcAreaSumFraction + ") > burnableAreaFraction * 1.2 (" + burnableAreaFraction * 1.2 + ") in first half");
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
            if (inLcClass) {
                baInLc.get(currentLcClass)[targetGridCellIndex] += burnedArea;
                return;
            }
        }
    }

    @Override
    protected float getErrorPerPixel(double[] probabilityOfBurn, double gridCellArea, double burnedPercentage) {
        // Mask all pixels with value 255 in the confidence level (corresponding to the pixels not observed or non-burnable in the JD layer)
        // From the remaining pixels, reassign all values of 0 to 1

        double[] probabilityOfBurnMasked = Arrays.stream(probabilityOfBurn)
                .map(d -> d == 0 ? 1.0 : d)
                .filter(d -> d <= 100.0 && d >= 1.0)
                .toArray();

//        System.out.println(Arrays.stream(probabilityOfBurn).filter(d -> d == 0).count());
//        System.out.println(Arrays.stream(probabilityOfBurn).filter(d -> d > 100).count());
//        System.out.println(probabilityOfBurn.length);
//        System.out.println(probabilityOfBurnMasked.length);
//        System.out.println(Arrays.toString(Arrays.stream(probabilityOfBurn).filter(d -> d <= 100.0 && d >= 1.0).toArray()));

        // n is the number of pixels in the 0.25º cell that were not masked
        int n = probabilityOfBurnMasked.length;

        if (n == 1) {
            return (float) ModisFireGridDataSource.MODIS_AREA_SIZE;
        }

        // pb_i = value of confidence level of pixel /100
        double[] pb = Arrays.stream(probabilityOfBurnMasked).map(d -> d / 100.0).toArray();

        // Var_c = sum (pb_i*(1-pb_i)
        double var_c = Arrays.stream(pb)
                .map(pb_i -> (pb_i * (1.0 - pb_i)))
                .sum();

        // SE = sqr(var_c*(n/(n-1))) * pixel area
        // pixel area is the area of the pixels. In the case of MODIS it is a constant value of 53664.6708 m2
        return (float) (Math.sqrt(var_c * (n / (n - 1.0))) * ModisFireGridDataSource.MODIS_AREA_SIZE);
    }

    @Override
    protected void predict(double[] ba, double[] areas, float[] originalErrors) {
        // just keep the original errors
    }

}