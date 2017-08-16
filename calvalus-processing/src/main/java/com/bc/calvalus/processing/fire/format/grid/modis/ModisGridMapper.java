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

    private static final float MODIS_PIXEL_AREA = 250.0F * 250.0F;

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
        String targetCell = paths[paths.length - 1].getName();
        LOG.info("targetCell=" + targetCell);

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

        String[] xCoords = getXCoords(targetCell);
        List<ZipFile> geoLookupTables = new ArrayList<>();
        for (String xCoord : xCoords) {
            Path geoLookup = new Path("hdfs://calvalus/calvalus/projects/fire/aux/modis-geolookup/modis-geo-luts-" + xCoord + ".zip");
            File localGeoLookup = CalvalusProductIO.copyFileToLocal(geoLookup, context.getConfiguration());
            geoLookupTables.add(new ZipFile(localGeoLookup));
        }

        ModisFireGridDataSource dataSource = new ModisFireGridDataSource(sourceProducts.toArray(new Product[0]), lcProducts.toArray(new Product[0]), geoLookupTables, targetCell, context.getConfiguration());
        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);
        dataSource.setDoyFirstHalf(doyFirstHalf);
        dataSource.setDoySecondHalf(doySecondHalf);

        setDataSource(dataSource);
        GridCell gridCell = computeGridCell(year, month);

        context.write(new Text(String.format("%d-%02d-%s", year, month, targetCell)), gridCell);
    }

    static String[] getXCoords(String targetTile) {
        String x = targetTile.split(",")[0];
        List<String> xCoords = new ArrayList<>();
        int xAsInt = Integer.parseInt(x);
        if (xAsInt % 8 != 0) {
            throw new IllegalArgumentException("Invalid input: '" + targetTile + "'");
        }
        for (int x0 = xAsInt; x0 < xAsInt + 8; x0++) {
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
        /*
            p is array of burned_probability in cell c
            var(C) = (p (1-p)).sum()
            standard_error(c) = sqrt(var(c) *(n/(n-1))
            sum(C) = p.sum()
        */

        double var_c = 0.0;
        double sum_c = 0.0;
        int count = 0;
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

        return (float) Math.sqrt(var_c * (count / (count - 1.0))) * MODIS_PIXEL_AREA;
    }

    @Override
    protected void predict(float[] ba, double[] areas, float[] originalErrors) {
        // just keep the original errors
    }


}