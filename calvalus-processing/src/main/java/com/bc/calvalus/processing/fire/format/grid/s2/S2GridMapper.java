package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.LcRemappingS2;
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
 * Runs the fire S2 formatting grid mapper.
 *
 * @author thomas
 */
public class S2GridMapper extends AbstractGridMapper {

    private static final int GRID_CELLS_PER_DEGREE = 4;
    private static final int NUM_GRID_CELLS = 2;
    private Context context;

    protected S2GridMapper() {
        super(NUM_GRID_CELLS * GRID_CELLS_PER_DEGREE, NUM_GRID_CELLS * GRID_CELLS_PER_DEGREE);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        this.context = context;
        int year = Integer.parseInt(context.getConfiguration().get("calvalus.year"));
        int month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        Path[] paths = inputSplit.getPaths();
        if (paths.length == 1) {
            return;
        }
        LOG.info("paths=" + Arrays.toString(paths));

        List<ZipFile> geoLookupTables = new ArrayList<>();
        String twoDegTile = paths[paths.length - 1].getName();

        List<Product> sourceProducts = new ArrayList<>();
        List<Product> lcProducts = new ArrayList<>();
        ProgressSplitProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        pm.beginTask("Copying data and computing grid cells...", targetRasterWidth * targetRasterHeight + paths.length - 1);
        for (int i = 0; i < paths.length - 1; i++) {
            String utmTile = paths[i].getName().substring(4, 9);
            String localGeoLookupFileName = twoDegTile + "-" + utmTile + ".zip";
            Path geoLookup = new Path("hdfs://calvalus/calvalus/projects/fire/aux/s2-geolookup/" + localGeoLookupFileName);
            if (!new File(localGeoLookupFileName).exists()) {
                File localGeoLookup = CalvalusProductIO.copyFileToLocal(geoLookup, context.getConfiguration());
                geoLookupTables.add(new ZipFile(localGeoLookup));
            }
            File sourceProductFile = CalvalusProductIO.copyFileToLocal(paths[i], context.getConfiguration());
            Product sourceProduct = ProductIO.readProduct(sourceProductFile);
            sourceProducts.add(sourceProduct);

            if (sourceProduct == null) {
                throw new IllegalStateException("Product " + sourceProductFile + " is broken.");
            }

            Path lcPath = new Path("hdfs://calvalus/calvalus/projects/fire/aux/lc4s2-from-s2/lc-2010-T" + utmTile + ".nc");
            File lcFile = new File(".", lcPath.getName());
            if (!lcFile.exists()) {
                CalvalusProductIO.copyFileToLocal(lcPath, lcFile, context.getConfiguration());
            }
            Product lcProduct = ProductIO.readProduct(lcFile);
            if (lcProduct == null) {
                throw new IllegalStateException("LC Product " + lcFile + " is broken.");
            }
            lcProducts.add(lcProduct);
            LOG.info(String.format("Loaded %02.2f%% of input products", (i + 1) * 100 / (float) (paths.length - 1)));
            pm.worked(1);
        }


        int doyFirstOfMonth = Year.of(year).atMonth(month).atDay(1).getDayOfYear();
        int doyLastOfMonth = Year.of(year).atMonth(month).atDay(Year.of(year).atMonth(month).lengthOfMonth()).getDayOfYear();

        S2FireGridDataSource dataSource = new S2FireGridDataSource(twoDegTile, sourceProducts.toArray(new Product[0]), lcProducts.toArray(new Product[0]), geoLookupTables);
        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);

        setDataSource(dataSource);
        GridCells gridCells = computeGridCells(year, month, pm);

        context.write(new Text(String.format("%d-%02d-%s", year, month, twoDegTile)), gridCells);
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
    protected float getErrorPerPixel(double[] probabilityOfBurn, double averageArea, int numberOfBurnedPixels, double burnedArea) {
        try (FileOutputStream fos = new FileOutputStream("prob-burn.json")) {
            try {
                ObjectOutputStream oos = new ObjectOutputStream(fos);
                oos.writeObject(probabilityOfBurn);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            File fileLocation = new File("./" + "prob-burn.json");
            Path path = new Path("hdfs://calvalus/calvalus/home/thomas/fire/prob-burn-s2.json");
            FileSystem fs = path.getFileSystem(context.getConfiguration());
            if (!fs.exists(path)) {
                FileUtil.copy(fileLocation, fs, path, false, context.getConfiguration());
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        System.out.println(averageArea);
        System.out.println(numberOfBurnedPixels);

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
    protected void predict(double[] ba, double[] areas, float[] originalErrors) {
    }

    @Override
    protected int getLcClassesCount() {
        return 6;
    }

    @Override
    protected void addBaInLandCover(List<double[]> baInLc, int targetGridCellIndex, double burnedArea, int sourceLc) {
        boolean inBurnableClass = LcRemappingS2.isInBurnableLcClass(sourceLc);
        if (sourceLc <= baInLc.size()) {
            baInLc.get(sourceLc - 1)[targetGridCellIndex] += inBurnableClass ? burnedArea : 0.0F;
        }
    }
}