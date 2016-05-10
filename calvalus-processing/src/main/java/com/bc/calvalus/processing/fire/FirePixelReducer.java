package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.ceres.binding.ConversionException;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.ProgressMonitorWrapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.esa.snap.core.util.converters.JtsGeometryConverter;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
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

    private FirePixelProductArea area;
    private FirePixelVariableType variableType;
    private NetcdfFileWriter ncFile;
    private String intermediateFilename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        area = FirePixelProductArea.valueOf(context.getConfiguration().get("area"));
        variableType = FirePixelVariableType.valueOf(context.getConfiguration().get("variableType"));
        prepareTargetProduct();
    }

    @Override
    protected void reduce(Text key, Iterable<PixelCell> values, Context context) throws IOException, InterruptedException {
        Iterator<PixelCell> iterator = values.iterator();
        PixelCell pixelCell = iterator.next();

        int x = getX(area, key.toString());
        int y = getY(area, key.toString());

        CalvalusLogger.getLogger().info(String.format("Writing data: x=%d, y=%d, 3600*3600 (tile %s) into variable '%s'", x, y, key, variableType.bandName));

        Array data = Array.factory(DataType.INT, new int[]{3600, 3600}, pixelCell.values);
        try {
            ncFile.write(ncFile.findVariable(variableType.bandName), new int[]{y, x}, data);
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        ncFile.close();
        String year = context.getConfiguration().get("calvalus.year");
        String month = context.getConfiguration().get("calvalus.month");

        CalvalusLogger.getLogger().info("Reading intermediate product...");
        Product intermediate = ProductIO.readProduct(intermediateFilename);
        SubsetOp subsetOp = new SubsetOp();
        getGeoRegion(subsetOp);
        subsetOp.setSourceProduct(intermediate);
        Product subset = subsetOp.getTargetProduct();
        String baseFilename = createBaseFilename(year, month, area, variableType.bandName);
        File fileLocation = new File("./" + baseFilename + ".tif");
        CalvalusLogger.getLogger().info("Writing final product...");
        ProductIO.writeProduct(subset, fileLocation, "GeoTIFF-BigTIFF", false, new ProgressMonitorWrapper(ProgressMonitor.NULL) {
            @Override
            public void worked(int work) {
                context.progress();
            }
        });
        subset.closeIO();

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

        int sceneRasterWidth = computeFullTargetWidth(area);
        int sceneRasterHeight = computeFullTargetHeight(area);
        intermediateFilename = "intermediate_" + variableType.bandName + ".nc";
        ncFile = createNcFile(intermediateFilename, sceneRasterWidth, sceneRasterHeight);

        try {
            writeLat(sceneRasterHeight);
            writeLon(sceneRasterWidth);
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
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

    private void writeLat(int sceneRasterHeight) throws IOException, InvalidRangeException {
        Variable lat = ncFile.findVariable("lat");
        float[] array = new float[sceneRasterHeight];
        array[0] = (area.top / 10) * 10.0F;
        for (int x = 1; x < sceneRasterHeight; x++) {
            array[x] = array[x - 1] + 1 / 360.0F;
        }
        Array values = Array.factory(DataType.FLOAT, new int[]{sceneRasterHeight}, array);
        ncFile.write(lat, values);
    }

    private void writeLon(int sceneRasterWidth) throws IOException, InvalidRangeException {
        Variable lon = ncFile.findVariable("lon");
        float[] array = new float[sceneRasterWidth];
        array[0] = (area.left / 10) * 10.0F - 180.0F;
        for (int x = 1; x < sceneRasterWidth; x++) {
            array[x] = array[x - 1] + 1 / 360.0F;
        }
        Array values = Array.factory(DataType.FLOAT, new int[]{sceneRasterWidth}, array);
        ncFile.write(lon, values);
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
