package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.CommonUtils;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.AreaCalculator;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.esa.snap.core.util.ProductUtils;

import java.io.File;
import java.io.IOException;
import java.time.Year;
import java.util.ArrayList;
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

        ModisFireGridDataSource dataSource = new ModisFireGridDataSource(sourceProducts, lcProducts, targetCell, context.getConfiguration());
        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);

        setDataSource(dataSource);
        GridCells gridCells = computeGridCells(year, month, pm);

        context.write(new Text(String.format("%d-%02d-%s", year, month, targetCell)), gridCells);
    }

    static String[] getXCoords(String targetTile) {
        String x = targetTile.split(",")[0];
        List<String> xCoords = new ArrayList<>();
        int xAsInt = Integer.parseInt(x);
        if (xAsInt % WINDOW_SIZE != 0) {
            throw new IllegalArgumentException("Invalid input: '" + targetTile + "'");
        }
        for (int x0 = xAsInt; x0 < xAsInt + WINDOW_SIZE; x0++) {
            String yCoord = Integer.toString(x0);
            if (yCoord.length() == 4) {
                maybeAddCoord(xCoords, yCoord.substring(0, 3) + "x");
            } else if (yCoord.length() == 3) {
                maybeAddCoord(xCoords, "0" + yCoord.substring(0, 2) + "x");
            } else if (yCoord.length() == 2) {
                maybeAddCoord(xCoords, "00" + yCoord.substring(0, 1) + "x");
            } else if (yCoord.length() == 1) {
                maybeAddCoord(xCoords, "000x");
            } else {
                throw new IllegalArgumentException("Invalid input: '" + targetTile + "'");
            }
        }
        return xCoords.toArray(new String[0]);

    }

    private static void maybeAddCoord(List<String> xCoords, String coord) {
        if (!xCoords.contains(coord)) {
            xCoords.add(coord);
        }
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
    protected float getErrorPerPixel(double[] probabilityOfBurn, double area, int numberOfBurnedPixels, double burnedArea) {
        double[] values = Arrays.stream(probabilityOfBurn)
                .map(d -> d == 0 ? 1 : d)
                .filter(d -> d >= 1 && d <= 100)
                .map(operand -> operand / 100.0).toArray();

        double sum_pb = 0.0;
        for (double p : values) {
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
            sum_pb += p;
        }

        double S = numberOfBurnedPixels / sum_pb;

        double[] pb_i_star = new double[values.length];

        for (int i = 0; i < values.length; i++) {
            double pb_i = values[i];
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

        if (Math.abs(checksum - numberOfBurnedPixels) > 0.0001) {
            throw new IllegalStateException(String.format("Math.abs(checksum (%s) - numberOfBurnedPixels (%s)) > 0.0001", checksum, numberOfBurnedPixels));
        }

        double var_c = 0.0;
        int count = 0;
        for (double p : pb_i_star) {
            var_c += p * (1 - p);
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
            count++;
        }

        if (count == 0) {
            return 0;
        }
        if (count == 1) {
            return 1;
        }

        return (float) (Math.sqrt(var_c * (count / (count - 1.0))) * ModisFireGridDataSource.MODIS_AREA_SIZE);

    }

    @Override
    protected void predict(double[] ba, double[] areas, float[] originalErrors) {
        // just keep the original errors
    }

    @Override
    protected boolean mustHandleCoverageSpecifially(int x) {
        boolean northWest = (targetCell.equals("0,64") || targetCell.equals("0,96")) && x == 0;
        boolean northEast = (targetCell.equals("1408,64") || targetCell.equals("1408,96")) && x == 31;
        return northEast || northWest;
    }

    @Override
    protected double getSpecialBurnableFractionValue(int x, int y, Product lcProduct) throws IOException {
        // return sum of all burnable areas for the given grid cell

        SubsetOp subsetOp = new SubsetOp();
        Geometry geometry;
        double lon0 = (Integer.parseInt(targetCell.split(",")[0]) / 4.0 + x * 0.25) - 180.0;
        double lat0 = 90.0 - (Integer.parseInt(targetCell.split(",")[1]) / 4.0 + y * 0.25);
        try {
            geometry = new WKTReader().read(String.format("POLYGON ((%s %s, %s %s, %s %s, %s %s, %s %s))",
                    lon0, lat0,
                    lon0 + 0.25, lat0,
                    lon0 + 0.25, lat0 - 0.25,
                    lon0, lat0 - 0.25,
                    lon0, lat0));
        } catch (ParseException e) {
            throw new IllegalStateException(e);
        }

        subsetOp.setGeoRegion(geometry);
        subsetOp.setSourceProduct(lcProduct);
        Product lcSubset = subsetOp.getTargetProduct();
        AreaCalculator areaCalculator = new AreaCalculator(lcSubset.getSceneGeoCoding());

        double burnableFractionSum = 0.0;
        Band lcBand = lcSubset.getBand("band_1");
        int width = lcBand.getRasterWidth();
        int height = lcBand.getRasterHeight();
        int[] lcClasses = new int[width * height];
        lcBand.readPixels(0, 0, width, height, lcClasses);
        int pixelIndex = 0;
        for (int y0 = 0; y0 < height; y0++) {
            for (int x0 = 0; x0 < width; x0++) {
                if (LcRemapping.isInBurnableLcClass(lcClasses[pixelIndex])) {
                    burnableFractionSum += areaCalculator.calculatePixelSize(x0, y0, width - 1, height - 1);
                }
                pixelIndex++;
            }
        }

        return burnableFractionSum;
    }

}