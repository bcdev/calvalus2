package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.CommonUtils;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.GridCell;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.util.ProductUtils;

import java.io.File;
import java.io.IOException;
import java.time.Year;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipFile;

/**
 * Runs the fire MODIS formatting grid mapper.
 *
 * @author thomas
 */
public class ModisGridMapper extends AbstractGridMapper {

    public static final int WINDOW_SIZE = 32;

    public ModisGridMapper() {
        super(WINDOW_SIZE, WINDOW_SIZE);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        int year = Integer.parseInt(context.getConfiguration().get("calvalus.year"));
        int month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        Path[] paths = inputSplit.getPaths();
        if (paths.length == 1) {
            return;
        }
        LOG.info("paths=" + Arrays.toString(paths));
        String targetCell = paths[paths.length - 1].getName();
        LOG.info("targetCell=" + targetCell);

        int numProducts = (paths.length - 1) / 2;

        Product[] sourceProducts = new Product[numProducts];
        Product[] lcProducts = new Product[numProducts];
        Product[] areaProducts = new Product[numProducts];
        int productIndex = 0;
        for (int i = 0; i < paths.length - 1; i += 2) {
            File sourceProductFile = CalvalusProductIO.copyFileToLocal(paths[i], context.getConfiguration());
            File lcProductFile = CalvalusProductIO.copyFileToLocal(paths[i + 1], context.getConfiguration());
            Product p = ProductIO.readProduct(sourceProductFile);
            if (p == null) {
                throw new IllegalStateException("Cannot read file " + paths[i]);
            }
            if (p.getName().contains("h18")) {
                Product temp = new Product(p.getName(), p.getProductType(), p.getSceneRasterWidth(), p.getSceneRasterHeight());
                ProductUtils.copyGeoCoding(p, temp);
                CommonUtils.fixH18Band(p, temp, "classification");
                CommonUtils.fixH18BandByte(p, temp, "numObs1");
                CommonUtils.fixH18BandByte(p, temp, "numObs2");
                if (p.containsBand("uncertainty")) {
                    CommonUtils.fixH18Band(p, temp, "uncertainty");
                }
                p = temp;
            }
            sourceProducts[productIndex] = p;
            lcProducts[productIndex] = ProductIO.readProduct(lcProductFile);
            String modisTile = p.getName().split("_")[3];
            File areaProductFile = new File(modisTile + ".hdf");
            if (!areaProductFile.exists()) {
                CalvalusProductIO.copyFileToLocal(new Path("hdfs://calvalus/calvalus/projects/fire/aux/modis-areas-luts/areas-" + modisTile + ".nc"), areaProductFile, context.getConfiguration());
            }

//            areaProducts[productIndex] = ProductIO.readProduct(areaProductFile);
            productIndex++;
        }

        int doyFirstOfMonth = Year.of(year).atMonth(month).atDay(1).getDayOfYear();
        int doyLastOfMonth = Year.of(year).atMonth(month).atDay(Year.of(year).atMonth(month).lengthOfMonth()).getDayOfYear();
        int doyFirstHalf = Year.of(year).atMonth(month).atDay(7).getDayOfYear();
        int doySecondHalf = Year.of(year).atMonth(month).atDay(22).getDayOfYear();

        String[] xCoords = getXCoords(targetCell);
        List<ZipFile> geoLookupTables = new ArrayList<>();
        for (String xCoord : xCoords) {
            Path geoLookup = new Path("hdfs://calvalus/calvalus/projects/fire/aux/modis-geolookup/modis-geo-luts-" + xCoord + ".zip");
            File localGeoLookup = CalvalusProductIO.copyFileToLocal(geoLookup, context.getConfiguration());
            geoLookupTables.add(new ZipFile(localGeoLookup));
        }

        ModisFireGridDataSource dataSource = new ModisFireGridDataSource(sourceProducts, lcProducts, areaProducts, geoLookupTables, targetCell);
        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);
        dataSource.setDoyFirstHalf(doyFirstHalf);
        dataSource.setDoySecondHalf(doySecondHalf);

        setDataSource(dataSource);
        GridCell gridCell = computeGridCell(year, month, new ProgressSplitProgressMonitor(context));

        context.write(new Text(String.format("%d-%02d-%s", year, month, targetCell)), gridCell);
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
    protected boolean maskUnmappablePixels() {
        return false;
    }

    @Override
    protected void validate(float burnableAreaFraction, List<float[]> baInLcFirst, List<float[]> baInLcSecond, int targetPixelIndex, double area) {
        double lcAreaSum = 0.0F;
        for (float[] firstBaValues : baInLcFirst) {
            lcAreaSum += firstBaValues[targetPixelIndex];
        }
        float lcAreaSumFraction = getFraction(lcAreaSum, area);
        if (lcAreaSumFraction > burnableAreaFraction * 1.2) {
            throw new IllegalStateException("lcAreaSumFraction (" + lcAreaSumFraction + ") > burnableAreaFraction * 1.2 (" + burnableAreaFraction * 1.2 + ") in first half");
        }
        lcAreaSum = 0.0F;
        for (float[] secondBaValues : baInLcSecond) {
            lcAreaSum += secondBaValues[targetPixelIndex];
        }
        lcAreaSumFraction = getFraction(lcAreaSum, area);
        if (lcAreaSumFraction > burnableAreaFraction * 1.2) {
            throw new IllegalStateException("lcAreaSumFraction (" + lcAreaSumFraction + ") > burnableAreaFraction (" + burnableAreaFraction * 1.2 + ") in second half");
        }
    }

    @Override
    protected float getErrorPerPixel(double[] probabilityOfBurn) {
        /*
            p is array of burned_probability in cell c
            var(C) = (p (1-p)).sum()
            standard_error(c) = sqrt(var(c) *(n/(n-1))
            sum(C) = p.sum()
        */

        double[] p_b = correct(probabilityOfBurn);

        double var_c = 0.0;
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

        return (float) Math.sqrt(var_c * (count / (count - 1.0))) * (float) ModisFireGridDataSource.MODIS_AREA_SIZE;
    }

    private double[] correct(double[] probabilityOfBurn) {
        /*
            Correct standard error:
            S = numberburn_pixels / sum_pb
            pb' = pb/S
            Then use this pb' for the std err calculation.
        */

        int numberburn_pixels = 0;
        double sum_pb = 0.0;
        for (double p : probabilityOfBurn) {
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
            numberburn_pixels++;
            sum_pb += p;
        }
        double S = numberburn_pixels / sum_pb;
        double[] result = new double[probabilityOfBurn.length];
        for (int i = 0; i < probabilityOfBurn.length; i++) {
            result[i] = probabilityOfBurn[i] / S;
        }
        return result;
    }

    @Override
    protected void predict(float[] ba, double[] areas, float[] originalErrors) {
        // just keep the original errors
    }


}