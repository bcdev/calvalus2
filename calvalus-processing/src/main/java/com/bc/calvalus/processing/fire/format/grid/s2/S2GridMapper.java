package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.ErrorPredictor;
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

    S2GridMapper() {
        super(STEP * 4, STEP * 4);
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

        List<ZipFile> geoLookupTables = new ArrayList<>();
        String fiveDegTile = paths[paths.length - 1].getName();

        Product[] sourceProducts = new Product[paths.length - 1];
        for (int i = 0; i < paths.length - 2; i++) {
            String utmTile = paths[i].getName().substring(4, 9);
            Path geoLookup = new Path("hdfs://calvalus/calvalus/projects/fire/aux/geolookup/" + fiveDegTile + "-" + utmTile + ".zip");
            File localGeoLookup = CalvalusProductIO.copyFileToLocal(geoLookup, context.getConfiguration());
            geoLookupTables.add(new ZipFile(localGeoLookup));
            File sourceProductFile = CalvalusProductIO.copyFileToLocal(paths[i], context.getConfiguration());
            sourceProducts[i] = ProductIO.readProduct(sourceProductFile);
        }

        File file = CalvalusProductIO.copyFileToLocal(paths[paths.length - 2], context.getConfiguration());
        Product lcProduct = ProductIO.readProduct(file);
        setGcToLcProduct(lcProduct);

        setDataSource(new S2FireGridDataSource(fiveDegTile, sourceProducts, lcProduct, geoLookupTables, LOG));
        ErrorPredictor errorPredictor = new ErrorPredictor();
        GridCell gridCell = computeGridCell(year, month, errorPredictor);

        context.progress();

        context.write(new Text(String.format("%d-%02d-%s", year, month, fiveDegTile)), gridCell);
        errorPredictor.dispose();
    }

    @Override
    protected boolean maskUnmappablePixels() {
        return false;
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

}