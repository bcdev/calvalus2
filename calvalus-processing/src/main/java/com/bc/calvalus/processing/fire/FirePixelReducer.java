package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.ceres.binding.ConversionException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.esa.snap.core.util.converters.JtsGeometryConverter;
import org.esa.snap.dataio.bigtiff.BigGeoTiffProductWriterPlugIn;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author thomas
 */
public class FirePixelReducer extends Reducer<Text, PixelCell, NullWritable, NullWritable> {

    private Product product;
    private FirePixelProductArea area;
    private FirePixelVariableType variableType;
    private String year;
    private String month;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        area = FirePixelProductArea.valueOf(context.getConfiguration().get("area"));
        variableType = FirePixelVariableType.valueOf(context.getConfiguration().get("variableType"));
        prepareTargetProduct();
        year = context.getConfiguration().get("calvalus.year");
        month = context.getConfiguration().get("calvalus.month");
    }

    @Override
    protected void reduce(Text key, Iterable<PixelCell> values, Context context) throws IOException, InterruptedException {
        Iterator<PixelCell> iterator = values.iterator();
        PixelCell pixelCell = iterator.next();

        int leftTargetX = getLeftTargetXForTile(area, key.toString());
        int topTargetY = getTopTargetYForTile(area, key.toString());

        int maxX = getMaxX(area, key.toString());
        int maxY = getMaxY(area, key.toString());

        Band band = product.getBand(variableType.bandName);
        Rectangle targetRect = computeTargetRect(area);
        if (band.getData() == null) {
            band.setData(new ProductData.Int(targetRect.width * targetRect.height));
        }

        CalvalusLogger.getLogger().info(String.format("Writing data: x=%d, y=%d, %d*%d (tile %s) into variable '%s'", leftTargetX, topTargetY, maxX - leftTargetX, maxY - topTargetY, key, variableType.bandName));

        int[] data = new int[(maxX - leftTargetX) * (maxY - topTargetY)];
        for (int y = topTargetY; y <= maxY; y++) {
            System.arraycopy(pixelCell.values, y * leftTargetX, data, y * leftTargetX, maxX - leftTargetX);
        }

        band.writeRasterData(leftTargetX, topTargetY, maxX - leftTargetX, maxY - topTargetY, new ProductData.Int(data));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String baseFilename = createBaseFilename(year, month, area, variableType.bandName);
        File fileLocation = new File("./" + baseFilename + ".tif");

        CalvalusLogger.getLogger().info("Copying final product...");
        String outputDir = context.getConfiguration().get("calvalus.output.dir");
        Path path = new Path(outputDir + "/" + fileLocation.getName());
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        FileUtil.copy(fileLocation, fs, path, false, context.getConfiguration());
        CalvalusLogger.getLogger().info("...done.");
    }

    private void getGeoRegion(SubsetOp subsetOp) throws IOException {
        int leftLon = area.left - 180;
        int upperLat = 90 - area.top;
        int rightLon = area.right - 180;
        int lowerLat = 90 - area.bottom;
        try {
            subsetOp.setGeoRegion(new JtsGeometryConverter().parse(String.format("POLYGON((%d %d, %d %d, %d %d, %d %d, %d %d))", leftLon, upperLat, leftLon, lowerLat, rightLon, lowerLat, rightLon, upperLat, leftLon, upperLat)));
        } catch (ConversionException e) {
            throw new IOException(e);
        }
    }

    private void prepareTargetProduct() throws IOException {
        int sceneRasterWidth = computeTargetWidth(area);
        int sceneRasterHeight = computeTargetHeight(area);

        String baseFilename = createBaseFilename(year, month, area, variableType.bandName);

        product = new Product(baseFilename, "Fire_CCI-Pixel Product", sceneRasterWidth, sceneRasterHeight);
        product.addBand(variableType.bandName, ProductData.TYPE_INT32);
        product.setSceneGeoCoding(createGeoCoding(area, sceneRasterWidth, sceneRasterHeight));
        File fileLocation = new File("./" + baseFilename + ".tif");
        product.setFileLocation(fileLocation);
        product.setProductWriter(new BigGeoTiffProductWriterPlugIn().createWriterInstance());
        product.getProductWriter().writeProductNodes(product, product.getFileLocation());
    }

    private NetcdfFileWriter createNcFile(String filename, int sceneRasterWidth, int sceneRasterHeight) throws IOException {
        NetcdfFileWriter ncFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf4_classic, filename);

        ncFile.addDimension(null, "lat", sceneRasterHeight);
        ncFile.addDimension(null, "lon", sceneRasterWidth);

        Variable latVar = ncFile.addVariable(null, "lat", DataType.FLOAT, "lat");
        latVar.addAttribute(new Attribute("units", "degree_north"));
        latVar.addAttribute(new Attribute("standard_name", "latitude"));
        latVar.addAttribute(new Attribute("long_name", "latitude"));
        latVar.addAttribute(new Attribute("bounds", "lat_bnds"));
        Variable lonVar = ncFile.addVariable(null, "lon", DataType.FLOAT, "lon");
        lonVar.addAttribute(new Attribute("units", "degree_east"));
        lonVar.addAttribute(new Attribute("standard_name", "longitude"));
        lonVar.addAttribute(new Attribute("long_name", "longitude"));
        lonVar.addAttribute(new Attribute("bounds", "lon_bnds"));

        ncFile.addVariable(null, variableType.bandName, DataType.INT, "lon lat");
        ncFile.create();
        return ncFile;
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

    static int computeTargetWidth(FirePixelProductArea area) {
        return (area.right - area.left) * 360;
    }

    static int computeTargetHeight(FirePixelProductArea area) {
        return (area.bottom - area.top) * 360;
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

    static String createBaseFilename(String year, String month, FirePixelProductArea area, String bandName) {
        return String.format("%s%s01-ESACCI-L3S_FIRE-BA-MERIS-AREA_%d-v02.0-fv04.0-%s", year, month, area.index, bandName);
    }

    static int getMaxX(FirePixelProductArea area, String key) {
        int tileX = Integer.parseInt(key.substring(12));
        if (tileX * 10 + 10 < area.right) {
            if (tileX * 10 < area.left) {
                return 360 * (area.left - tileX * 10) - 1;
            } else {
                return getLeftTargetXForTile(area, key) + 3600 - 1;
            }
        } else if (tileX * 10 + 10 == area.right) {
            return getLeftTargetXForTile(area, key) + 3600 - 1;
        }
        return getLeftTargetXForTile(area, key) + 360 * (area.right - tileX * 10) - 1;
    }

    static int getMaxY(FirePixelProductArea area, String key) {
        int tileY = Integer.parseInt(key.substring(9, 11));
        if (tileY * 10 + 10 < area.bottom) {
            if (tileY * 10 < area.top) {
                return 360 * (area.top - tileY * 10) - 1;
            } else {
                return getTopTargetYForTile(area, key) + 3600 - 1;
            }
        } else if (tileY * 10 + 10 == area.bottom) {
            return getTopTargetYForTile(area, key) + 3600 - 1;
        }
        return getTopTargetYForTile(area, key) + 360 * (area.bottom - tileY * 10) - 1;
    }

    static int getLeftTargetXForTile(FirePixelProductArea area, String key) {
        int tileX = Integer.parseInt(key.substring(12));
        if (tileX * 10 > area.left) {
            return (tileX * 10 - area.left) * 360;
        }
        return 0;
    }

    static int getTopTargetYForTile(FirePixelProductArea area, String key) {
        int tileY = Integer.parseInt(key.substring(9, 11));
        if (tileY * 10 > area.top) {
            return (tileY * 10 - area.top) * 360;
        }
        return 0;
    }

    private static int _getX(FirePixelProductArea area, String key) {
        int x = Integer.parseInt(key.substring(12));
        int minX = area.left / 10;
        return (x - minX) * 3600;
    }

    private static int _getY(FirePixelProductArea area, String key) {
        int y = Integer.parseInt(key.substring(9, 11));
        int minY = area.top / 10;
        return (y - minY) * 3600;
    }
}
