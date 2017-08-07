package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.GridCell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;

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

    public ModisGridMapper() {
        super(8, 8);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        int year = Integer.parseInt(context.getConfiguration().get("calvalus.year"));
        int month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        Path[] paths = inputSplit.getPaths();
        if (paths.length == 1) {
            return;
        }
        LOG.info("paths=" + Arrays.toString(paths));
        String targetTile = paths[paths.length - 1].getName();
        LOG.info("targetTile=" + targetTile);

        List<Product> sourceProducts = new ArrayList<>();
        List<Product> lcProducts = new ArrayList<>();
        for (int i = 0; i < paths.length - 1; i += 2) {
            File sourceProductFile = CalvalusProductIO.copyFileToLocal(paths[i], context.getConfiguration());
            File lcProductFile = CalvalusProductIO.copyFileToLocal(paths[i + 1], context.getConfiguration());
            sourceProducts.add(ProductIO.readProduct(sourceProductFile));
            lcProducts.add(ProductIO.readProduct(lcProductFile));
        }

        int doyFirstOfMonth = Year.of(year).atMonth(month).atDay(1).getDayOfYear();
        int doyLastOfMonth = Year.of(year).atMonth(month).atDay(Year.of(year).atMonth(month).lengthOfMonth()).getDayOfYear();
        int doyFirstHalf = Year.of(year).atMonth(month).atDay(7).getDayOfYear();
        int doySecondHalf = Year.of(year).atMonth(month).atDay(22).getDayOfYear();

        String[] yCoords = getYCoords(targetTile);
        List<ZipFile> geoLookupTables = new ArrayList<>();
        for (String yCoord : yCoords) {
            Path geoLookup = new Path("hdfs://calvalus/calvalus/projects/fire/aux/modis-geolookup/modis-geo-luts-" + yCoord + ".zip");
            File localGeoLookup = CalvalusProductIO.copyFileToLocal(geoLookup, context.getConfiguration());
            geoLookupTables.add(new ZipFile(localGeoLookup));
        }

        ModisFireGridDataSource dataSource = new ModisFireGridDataSource(sourceProducts.toArray(new Product[0]), lcProducts.toArray(new Product[0]), geoLookupTables, targetTile);
        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);
        dataSource.setDoyFirstHalf(doyFirstHalf);
        dataSource.setDoySecondHalf(doySecondHalf);

        setDataSource(dataSource);
        GridCell gridCell = computeGridCell(year, month);

        context.write(new Text(String.format("%d-%02d-%s", year, month, targetTile)), gridCell);
    }

    static String[] getYCoords(String targetTile) {
        String y = targetTile.split(",")[0];
        List<String> yCoords = new ArrayList<>();
        int yAsInt = Integer.parseInt(y);
        if (yAsInt % 8 != 0) {
            throw new IllegalArgumentException("Invalid input: '" + targetTile + "'");
        }
        for (int y0 = yAsInt; y0 < yAsInt + 8; y0++) {
            String yCoord = Integer.toString(y0);
            if (yCoord.length() == 4) {
                maybeAddCoord(yCoords, yCoord.substring(0, 3) + "x");
            } else if (yCoord.length() == 3) {
                maybeAddCoord(yCoords, "0" + yCoord.substring(0, 2) + "x");
            } else if (yCoord.length() == 2) {
                maybeAddCoord(yCoords, "00" + yCoord.substring(0, 1) + "x");
            } else if (yCoord.length() == 1) {
                maybeAddCoord(yCoords, "000x");
            } else {
                throw new IllegalArgumentException("Invalid input: '" + targetTile + "'");
            }
        }
        return yCoords.toArray(new String[0]);

    }

    private static void maybeAddCoord(List<String> yCoords, String coord) {
        if (!yCoords.contains(coord)) {
            yCoords.add(coord);
        }
    }

    @Override
    protected boolean maskUnmappablePixels() {
        return false;
    }

    @Override
    protected void validate(float burnableFraction, List<float[]> baInLcFirst, List<float[]> baInLcSecond, int targetPixelIndex, double area) {
        double lcAreaSum = 0.0F;
        for (int i = 0; i < baInLcFirst.size(); i++) {
            float[] firstBaValues = baInLcFirst.get(i);
            float[] secondBaValues = baInLcSecond.get(i);
            lcAreaSum += firstBaValues[targetPixelIndex];
            lcAreaSum += secondBaValues[targetPixelIndex];
        }
        float lcAreaSumFraction = getFraction(lcAreaSum, area);
        if (Math.abs(lcAreaSumFraction - burnableFraction) > lcAreaSumFraction * 0.05) {
            throw new IllegalStateException("fraction of burned pixels in LC classes (" + lcAreaSumFraction + ") > burnable fraction (" + burnableFraction + ") at target pixel " + targetPixelIndex + "!");
        }
    }

    @Override
    protected float getErrorPerPixel(double[] probabilityOfBurn) {
//        np.sqrt(((1-a)*a).sum())
        double sum = 0.0;
        for (double p : probabilityOfBurn) {
            if (Double.isNaN(p)) {
                continue;
            }
            if (p > 1) {
                continue;
            }
            if (p < 0) {
                throw new IllegalStateException("p < 0");
            }
            sum += (1 - p) * p;
        }
        return (float) Math.sqrt(sum);

        /*

        LOG.info(String.format("Starting to compute errors...."));
        double[] pdf = UncertaintyEngine.poisson_binomial(probabilityOfBurn);
        LOG.info(String.format("done. Writing result..."));
        String year = context.getConfiguration().get("calvalus.year");
        String month = context.getConfiguration().get("calvalus.month");
        String filename = String.format("pdf-%s-%s-%s-%s.csv", year, month, firstHalf ? "07" : "22", count++);
        try (FileWriter fw = new FileWriter(filename)) {
            CsvWriter csvWriter = new CsvWriter(fw, ";");
            csvWriter.writeRecord(pdf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info(String.format("done. Exporting result..."));
        try {
            CalvalusProductIO.copyFileToLocal(new Path("hdfs://calvalus/calvalus/home/thomas/", filename), new File(filename), context.getConfiguration());
            new File(filename).delete();
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info(String.format("done."));
        firstHalf = false;
        return 0;
        */
    }

    @Override
    protected void predict(float[] ba, double[] areas, float[] originalErrors) {
        // just keep the original errors
    }



}