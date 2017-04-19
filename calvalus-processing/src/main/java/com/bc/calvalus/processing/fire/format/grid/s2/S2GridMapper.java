package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.GridCell;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipFile;

import static com.bc.calvalus.processing.fire.format.grid.s2.S2FireGridDataSource.STEP;

/**
 * Runs the fire S2 formatting grid mapper.
 *
 * @author thomas
 */
public class S2GridMapper extends AbstractGridMapper {

    private Context context;
    private boolean firstHalf = true;
    static int count = 0;

    S2GridMapper() {
        super(STEP * 4, STEP * 4);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        this.context = context;
        int year = Integer.parseInt(this.context.getConfiguration().get("calvalus.year"));
        int month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        Path[] paths = inputSplit.getPaths();
        if (paths.length == 1) {
            return;
        }
        LOG.info("paths=" + Arrays.toString(paths));

        List<ZipFile> geoLookupTables = new ArrayList<>();
        String fiveDegTile = paths[paths.length - 1].getName();

        List<Product> sourceProducts = new ArrayList<>();
        for (int i = 0; i < paths.length - 2; i++) {
            String utmTile = paths[i].getName().substring(4, 9);
            String localGeoLookupFileName = fiveDegTile + "-" + utmTile + ".zip";
            Path geoLookup = new Path("hdfs://calvalus/calvalus/projects/fire/aux/geolookup/" + localGeoLookupFileName);
            if (!new File(localGeoLookupFileName).exists()) {
                File localGeoLookup = CalvalusProductIO.copyFileToLocal(geoLookup, context.getConfiguration());
                geoLookupTables.add(new ZipFile(localGeoLookup));
            }
            File sourceProductFile = CalvalusProductIO.copyFileToLocal(paths[i], context.getConfiguration());
            sourceProducts.add(ProductIO.readProduct(sourceProductFile));
            LOG.info(String.format("Loaded %02.2f%% of input products", (i + 1) * 100 / (float) (paths.length - 2)));
        }

        File file = CalvalusProductIO.copyFileToLocal(paths[paths.length - 2], context.getConfiguration());
        Product lcProduct = ProductIO.readProduct(file);
        setGcToLcProduct(lcProduct);

        int doyFirstOfMonth = Year.of(year).atMonth(month).atDay(1).getDayOfYear();
        int doyLastOfMonth = Year.of(year).atMonth(month).atDay(Year.of(year).atMonth(month).lengthOfMonth()).getDayOfYear();
        int doyFirstHalf = Year.of(year).atMonth(month).atDay(7).getDayOfYear();
        int doySecondHalf = Year.of(year).atMonth(month).atDay(22).getDayOfYear();

        S2FireGridDataSource dataSource = new S2FireGridDataSource(fiveDegTile, sourceProducts.toArray(new Product[0]), lcProduct, geoLookupTables);
        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);
        dataSource.setDoyFirstHalf(doyFirstHalf);
        dataSource.setDoySecondHalf(doySecondHalf);

        setDataSource(dataSource);
        GridCell gridCell = computeGridCell(year, month);

        context.write(new Text(String.format("%d-%02d-%s", year, month, fiveDegTile)), gridCell);
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

    public static void setGcToLcProduct(Product lcProduct) throws IOException {
        String tile = lcProduct.getName().substring(8, 14);
        int tileX = Integer.parseInt(tile.substring(4, 6));
        int tileY = Integer.parseInt(tile.substring(1, 3));
        int easting = 10 * tileX - 180;
        int northing = 90 - 10 * tileY;
        int height = lcProduct.getSceneRasterHeight();
        int width = lcProduct.getSceneRasterWidth();
        double pixelSize = 1 / 360.0;
        CrsGeoCoding sceneGeoCoding;
        try {
            sceneGeoCoding = new CrsGeoCoding(DefaultGeographicCRS.WGS84, width, height, easting, northing, pixelSize, pixelSize);
        } catch (FactoryException | TransformException e) {
            throw new IllegalStateException("Cannot construct geo-coding for LC tile.", e);
        }
        lcProduct.setSceneGeoCoding(sceneGeoCoding);
    }

    @Override
    protected float getErrorPerPixel(float[] ba, double[] probabilityOfBurn) {
//        np.sqrt(((1-a)*a).sum())
        double sum = 0.0;
        for (double a : probabilityOfBurn) {
            sum += (1 - a) * a;
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