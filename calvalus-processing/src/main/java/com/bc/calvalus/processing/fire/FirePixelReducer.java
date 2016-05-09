package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.dimap.DimapProductWriterPlugIn;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * @author thomas
 */
public class FirePixelReducer extends Reducer<Text, PixelCell, NullWritable, NullWritable> {

    private Product product;
    private FirePixelProductArea area;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        prepareTargetProduct(context);
    }

    @Override
    protected void reduce(Text key, Iterable<PixelCell> values, Context context) throws IOException, InterruptedException {
        Iterator<PixelCell> iterator = values.iterator();
        PixelCell pixelCell = iterator.next();

        int[] doy = pixelCell.doy;
        int[] error = pixelCell.error;
        int[] lcClass = pixelCell.lcClass;

        int x = getX(area, key.toString());
        int y = getY(area, key.toString());

        CalvalusLogger.getLogger().info(String.format("Writing data: x=%d, y=%d, 3600*3600 (tile %s) into variable 'JD'", x, y, key));
        product.getBand("JD").writeRasterData(x, y, 3600, 3600, new ProductData.Int(doy));

        CalvalusLogger.getLogger().info(String.format("Writing data: x=%d, y=%d, 3600*3600 (tile %s) into variable 'CL'", x, y, key));
        product.getBand("CL").writeRasterData(x, y, 3600, 3600, new ProductData.Int(error));

        CalvalusLogger.getLogger().info(String.format("Writing data: x=%d, y=%d, 3600*3600 (tile %s) into variable 'LC'", x, y, key));
        product.getBand("LC").writeRasterData(x, y, 3600, 3600, new ProductData.Int(lcClass));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        product.closeIO();

        String year = context.getConfiguration().get("calvalus.year");
        String month = context.getConfiguration().get("calvalus.month");

        String baseFilename = createBaseFilename(year, month, area);

        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("region", computeTargetRect(area));
        Product subset = GPF.createProduct("Subset", parameters, product);
        File fileLocation = new File("./" + baseFilename + ".tif");
        ProductIO.writeProduct(subset, fileLocation, "GeoTIFF", false);
        subset.closeIO();

        String outputDir = context.getConfiguration().get("calvalus.output.dir");
        Path path = new Path(outputDir + "/" + fileLocation.getName());
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        FileUtil.copy(fileLocation, fs, path, false, context.getConfiguration());
    }

    private void prepareTargetProduct(Context context) throws IOException {
        area = FirePixelProductArea.valueOf(context.getConfiguration().get("area"));

        int sceneRasterWidth = computeFullTargetWidth(area);
        int sceneRasterHeight = computeFullTargetHeight(area);
        String baseFilename = "intermediate";

        product = new Product("intermediate", "Fire_CCI-Pixel Product", sceneRasterWidth, sceneRasterHeight);
        product.addBand("JD", ProductData.TYPE_INT32);
        product.addBand("CL", ProductData.TYPE_INT32);
        product.addBand("LC", ProductData.TYPE_INT32);
        product.setSceneGeoCoding(createGeoCoding(area, sceneRasterWidth, sceneRasterHeight));
        product.setFileLocation(new File("./" + baseFilename + ".tif"));
        product.setProductWriter(new DimapProductWriterPlugIn().createWriterInstance());
        product.getProductWriter().writeProductNodes(product, product.getFileLocation());
    }

    private static CrsGeoCoding createGeoCoding(FirePixelProductArea area, int sceneRasterWidth, int sceneRasterHeight) throws IOException {
        double easting = area.left - 180;
        double northing = 90 - area.top;
        try {
            return new CrsGeoCoding(DefaultGeographicCRS.WGS84, sceneRasterWidth, sceneRasterHeight, easting, northing, 1 / 360.0, 1 / 360.0);
        } catch (FactoryException | TransformException e) {
            throw new IOException("Unable to create geo-coding", e);
        }
    }

    static int computeFullTargetWidth(FirePixelProductArea area) {
        boolean exactlyMatchesBorder = area.right % 10.0 == 0.0 || area.left % 10.0 == 0.0;
        return (area.right / 10 - area.left / 10 + (exactlyMatchesBorder ? 0 : 1)) * 3600;
    }

    static int computeFullTargetHeight(FirePixelProductArea area) {
        boolean exactlyMatchesBorder = area.top % 10.0 == 0.0 || area.bottom % 10.0 == 0.0;
        return (area.bottom / 10 - area.top / 10 + (exactlyMatchesBorder ? 0 : 1)) * 3600;
    }

    static Rectangle computeTargetRect(FirePixelProductArea area) {
        int x = (area.left - area.left / 10 * 10) * 360;
        int y = (area.top - area.top / 10 * 10) * 360;
        int width = (area.right - area.left) * 360;
        int height = (area.bottom - area.top) * 360;
        return new Rectangle(x, y, width, height);
    }

    static String createBaseFilename(String year, String month, FirePixelProductArea area) {
        return String.format("%s%s01-ESACCI-L3S_FIRE-BA-MERIS-AREA_%d-v02.0-fv04.0", year, month, area.index);
    }

    private static int getX(FirePixelProductArea area, String key) {
        int x = Integer.parseInt(key.substring(12));
        int minX = area.left / 10;
        return (x - minX) * 3600;
    }

    private static int getY(FirePixelProductArea area, String key) {
        int y = Integer.parseInt(key.substring(9, 11));
        int minY = area.top / 10;
        return (y - minY) * 3600;
    }
}
