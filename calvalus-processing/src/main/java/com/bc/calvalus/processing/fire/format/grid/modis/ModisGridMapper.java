package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.CommonUtils;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.util.ProductUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
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
    private Context context;

    public ModisGridMapper() {
        super(WINDOW_SIZE, WINDOW_SIZE);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        this.context = context;
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
        int productIndex = 0;
        for (int i = 0; i < paths.length - 1; i += 2) {
            if (paths[i].getName().contains("dummy")) {
                Product product = new Product(paths[i].getName(), "dummy", 4800, 4800);
                product.addBand("classification", "0", ProductData.TYPE_INT16);
                product.addBand("numObs1", "3", ProductData.TYPE_UINT8);
                sourceProducts[productIndex] = product;
            } else {
                File sourceProductFile = CalvalusProductIO.copyFileToLocal(paths[i], context.getConfiguration());
                Product p = ProductIO.readProduct(sourceProductFile);
                if (p == null) {
                    throw new IllegalStateException("Cannot read file " + paths[i]);
                }
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
            }
            File lcProductFile = CalvalusProductIO.copyFileToLocal(paths[i + 1], context.getConfiguration());
            lcProducts[productIndex] = ProductIO.readProduct(lcProductFile);

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

        ModisFireGridDataSource dataSource = new ModisFireGridDataSource(sourceProducts, lcProducts, geoLookupTables, targetCell);
        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);

        setDataSource(dataSource);
        GridCells gridCells = computeGridCells(year, month, new ProgressSplitProgressMonitor(context));

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
        int count = 0;
        for (int currentLcClass = 0; currentLcClass < getLcClassesCount(); currentLcClass++) {
            boolean inLcClass = LcRemapping.isInLcClass(currentLcClass + 1, sourceLc);
            if (inLcClass) {
                baInLc.get(currentLcClass)[targetGridCellIndex] += burnedArea;
                count++;
            }
        }
        if (count != 1) {
            throw new IllegalStateException("Burned area added to " + count + " LC classes!\n" +
                    "targetGridCellIndex=" + targetGridCellIndex + "\n" +
                    "burnedArea=" + burnedArea + "\n" +
                    "sourceLc=" + sourceLc);
        }
    }

    @Override
    protected float getErrorPerPixel(double[] probabilityOfBurn, double area, int numberOfBurnedPixels, double burnedArea) {
        try (FileOutputStream fos = new FileOutputStream("prob-burn.json")) {
            try {
                ObjectOutputStream oos = new ObjectOutputStream(fos);
                oos.writeObject(probabilityOfBurn);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            File fileLocation = new File("./" + "prob-burn.json");
            Path path = new Path("hdfs://calvalus/calvalus/home/thomas/fire/prob-burn.json");
            FileSystem fs = path.getFileSystem(context.getConfiguration());
            if (!fs.exists(path)) {
                FileUtil.copy(fileLocation, fs, path, false, context.getConfiguration());
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        System.out.println(probabilityOfBurn.length);
        System.out.println(area);
        System.out.println(numberOfBurnedPixels);
        System.out.println(burnedArea);

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
            sum_pb += p;
        }

        double S = numberOfBurnedPixels / sum_pb;

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

        return (float) Math.sqrt(var_c * (count / (count - 1.0))) * (float) ModisFireGridDataSource.MODIS_AREA_SIZE;

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
    protected void predict(double[] ba, double[] areas, float[] originalErrors) {
        // just keep the original errors
    }


}