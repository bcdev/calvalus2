package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.RasterStackWritable;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.ceres.core.Assert;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.binning.support.CrsGrid;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.Year;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;

import static com.bc.calvalus.processing.fire.LcRemapping.isInBurnableLcClass;
import static com.bc.calvalus.processing.fire.LcRemapping.remap;

public class FirePixelReducer extends Reducer<LongWritable, RasterStackWritable, NullWritable, NullWritable> {

    public static final int DEFAULT_NUM_GLOBAL_ROWS = 64800;
    private static final short BA_FILL_VALUE = -32767;
    protected NetcdfFileWriter ncFile;

    protected String ncFilename;
    private CrsGrid crsGrid;
    private int numRowsGlobal;
    private Product lcProduct;
    private Rectangle continentalRectangle;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        int lcYear = getLcYear(context);
        Product lcProduct = ProductIO.readProduct(String.format("/mnt/auxiliary/auxiliary/c3s/lc/C3S-LC-L4-LCCS-Map-300m-P1Y-%s-v2.1.1.nc", lcYear));
        try {
            init(context.getConfiguration(), lcProduct);
        } catch (FactoryException | TransformException e) {
            throw new RuntimeException(e);
        }
    }

    private static int getLcYear(Reducer<LongWritable, RasterStackWritable, NullWritable, NullWritable>.Context context) {
        int producedYear = context.getConfiguration().getInt("calvalus.year", 2019);
        return producedYear - 1;
    }

    void init(Configuration configuration, Product lcProduct) throws IOException, FactoryException, TransformException {
        numRowsGlobal = configuration.getInt("numRowsGlobal", DEFAULT_NUM_GLOBAL_ROWS);
        crsGrid = new CrsGrid(numRowsGlobal, "EPSG:4326");
        computeContinentalRectangle(configuration);
        this.lcProduct = lcProduct;
        prepareTargetProduct(configuration);
    }

    private void computeContinentalRectangle(Configuration configuration) throws FactoryException, TransformException {
        Geometry continentalGeometry = GeometryUtils.createGeometry(configuration.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        Product dummyProduct = new Product("dummy", "dummy", numRowsGlobal * 2, numRowsGlobal);
        dummyProduct.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, numRowsGlobal * 2, numRowsGlobal, -180, 90, 360.0 / (numRowsGlobal * 2), 180.0 / numRowsGlobal));
        continentalRectangle = SubsetOp.computePixelRegion(dummyProduct, continentalGeometry, 0);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<RasterStackWritable> values, Context context) throws IOException {
        Iterator<RasterStackWritable> iterator = values.iterator();
        RasterStackWritable rasterStackWritable = iterator.next();

        CalvalusLogger.getLogger().info("Writing for key " + key);

        long binIndex = key.get();

        byte[] lcData = new byte[rasterStackWritable.width * rasterStackWritable.height];
        short[] lcDataShort = new short[rasterStackWritable.width * rasterStackWritable.height];
        lcProduct.getBand("lccs_class").readRasterData(getX(binIndex), getY(binIndex), rasterStackWritable.width, rasterStackWritable.height, new ProductData.Byte(lcData));
        short[] baData = (short[]) rasterStackWritable.data[0];
        byte[] clData = (byte[]) rasterStackWritable.data[1];

        for (int i = 0; i < baData.length; i++) {
            lcDataShort[i] = (short) remap(lcData[i]);
            if (!isInBurnableLcClass(lcDataShort[i])) {
                baData[i] = -2;
                clData[i] = 0;
            }
            if (baData[i] == BA_FILL_VALUE) {
                // we are over water
                baData[i] = -2;
                clData[i] = 0;
            }
        }

        try {
            writeShortChunk(getX(binIndex) - continentalRectangle.x, getY(binIndex) - continentalRectangle.y, ncFile, "JD", baData, rasterStackWritable.width, rasterStackWritable.height);
            writeByteChunk(getX(binIndex) - continentalRectangle.x, getY(binIndex) - continentalRectangle.y, ncFile, "CL", clData, rasterStackWritable.width, rasterStackWritable.height);
            writeUByteChunk(getX(binIndex) - continentalRectangle.x, getY(binIndex) - continentalRectangle.y, ncFile, "LC", lcData, rasterStackWritable.width, rasterStackWritable.height);
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void cleanup(Reducer.Context context) throws IOException {
        String outputDir = context.getConfiguration().get("calvalus.output.dir");

        closeNcFile();

        File fileLocation = new File("./" + ncFilename);
        Path path = new Path(outputDir + "/" + ncFilename);
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        if (!fs.exists(path)) {
            FileUtil.copy(fileLocation, fs, path, false, context.getConfiguration());
        }
    }

    void closeNcFile() throws IOException {
        ncFile.close();
    }

    private void prepareTargetProduct(Configuration configuration) throws IOException, FactoryException, TransformException {
        String year = configuration.get("calvalus.year");
        String month = configuration.get("calvalus.month");
        String version = configuration.get("calvalus.version", "v5.1");
        Assert.notNull(year, "calvalus.year");
        Assert.notNull(month, "calvalus.month");

        int lastDayOfMonth = Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).lengthOfMonth();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneId.systemDefault());
        String timeCoverageStart = dtf.format(LocalDate.of(Integer.parseInt(year), Integer.parseInt(month), 1).atTime(0, 0, 0));
        String timeCoverageEnd = dtf.format(LocalDate.of(Integer.parseInt(year), Integer.parseInt(month), lastDayOfMonth).atTime(23, 59, 59));

        Geometry continentalGeometry = GeometryUtils.createGeometry(configuration.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        Product dummyProduct = new Product("dummy", "dummy", numRowsGlobal * 2, numRowsGlobal);
        dummyProduct.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, numRowsGlobal * 2, numRowsGlobal, -180, 90, 360.0 / (numRowsGlobal * 2), 180.0 / numRowsGlobal));
        continentalRectangle = SubsetOp.computePixelRegion(dummyProduct, continentalGeometry, 0);

        ncFilename = getFilename(year, month, version);
        ncFile = new FirePixelNcFactory().createNcFile(ncFilename, version, timeCoverageStart, timeCoverageEnd, lastDayOfMonth, continentalRectangle.width, continentalRectangle.height);

        try {
            writeLon(ncFile, continentalRectangle.x, continentalRectangle.width);
            writeLat(ncFile, continentalRectangle.y, continentalRectangle.height);

            writeLonBnds(ncFile, continentalRectangle.x, continentalRectangle.width);
            writeLatBnds(ncFile, continentalRectangle.y, continentalRectangle.height);

            writeTime(ncFile, year, month);
            writeTimeBnds(ncFile, year, month);

        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
    }

    protected void writeShortChunk(int x, int y, NetcdfFileWriter ncFile, String varName, short[] data, int width, int height) throws IOException, InvalidRangeException {
        CalvalusLogger.getLogger().info(String.format("Writing data: x=%d, y=%d, %d*%d into variable %s", x, y, width, height, varName));

        Variable variable = ncFile.findVariable(varName);
        Array values = Array.factory(DataType.SHORT, new int[]{1, height, width}, data);
        ncFile.write(variable, new int[]{0, y, x}, values);
    }

    protected void writeByteChunk(int x, int y, NetcdfFileWriter ncFile, String varName, byte[] data, int width, int height) throws IOException, InvalidRangeException {
        CalvalusLogger.getLogger().info(String.format("Writing data: x=%d, y=%d, %d*%d into variable %s", x, y, width, height, varName));

        Variable variable = ncFile.findVariable(varName);
        Array values = Array.factory(DataType.BYTE, new int[]{1, height, width}, data);
        ncFile.write(variable, new int[]{0, y, x}, values);
    }

    protected void writeUByteChunk(int x, int y, NetcdfFileWriter ncFile, String varName, byte[] data, int width, int height) throws IOException, InvalidRangeException {
        CalvalusLogger.getLogger().info(String.format("Writing data: x=%d, y=%d, %d*%d into variable %s", x, y, width, height, varName));

        Variable variable = ncFile.findVariable(varName);
        Array values = Array.factory(DataType.UBYTE, new int[]{1, height, width}, data);
        ncFile.write(variable, new int[]{0, y, x}, values);
    }

//    protected void writeUByteChunk(int x, int y, NetcdfFileWriter ncFile, String varName, byte[] data, int width, int height) throws IOException, InvalidRangeException {
//        CalvalusLogger.getLogger().info(String.format("Writing data: x=%d, y=%d, %d*%d into variable %s", x, y, width, height, varName));
//
//        Variable variable = ncFile.findVariable(varName);
//        Array values = Array.factory(DataType.UBYTE, new int[]{1, width, height}, data);
//        ncFile.write(variable, new int[]{0, y, x}, values);
//    }

    protected String getFilename(String year, String month, String version) {
        String paddedMonth = String.format("%02d", Integer.parseInt(month));
        return String.format("%s%s01-C3S-L4_FIRE-BA-OLCI-f%s.nc", year, paddedMonth, version);
    }

    private static void writeTimeBnds(NetcdfFileWriter ncFile, String year, String month) throws IOException, InvalidRangeException {
        Variable timeBnds = ncFile.findVariable("time_bounds");
        double firstDayAsJD = getFirstDayAsJD(year, month);
        int lengthOfMonth = Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).lengthOfMonth();
        float[] array = new float[]{
                (float) (firstDayAsJD),
                (float) (firstDayAsJD + lengthOfMonth - 1)};
        Array values = Array.factory(DataType.FLOAT, new int[]{1, 2}, array);
        ncFile.write(timeBnds, values);
    }

    private static void writeTime(NetcdfFileWriter ncFile, String year, String month) throws IOException, InvalidRangeException {
        Variable time = ncFile.findVariable("time");
        double firstDayAsJD = getFirstDayAsJD(year, month);
        double[] array = new double[]{firstDayAsJD};
        Array values = Array.factory(DataType.DOUBLE, new int[]{1}, array);
        ncFile.write(time, values);
    }

    private static double getFirstDayAsJD(String year, String month) {
        LocalDate current = Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).atDay(1);
        LocalDate epoch = Year.of(1970).atMonth(1).atDay(1);
        return ChronoUnit.DAYS.between(epoch, current);
    }

    private void writeLonBnds(NetcdfFileWriter ncFile, int continentalStartX, int continentalWidth) throws IOException, InvalidRangeException {
        float halfPixelSize = (float) (360.0 / (numRowsGlobal * 2));

        Variable lonBnds = ncFile.findVariable("lon_bounds");
        float[] array = new float[continentalWidth * 2];

        for (int x = 0; x < continentalWidth; x++) {
            array[2 * x] = (float) crsGrid.getCenterLatLon(x + continentalStartX)[1] - halfPixelSize;
            array[2 * x + 1] = (float) crsGrid.getCenterLatLon(x + continentalStartX)[1] + halfPixelSize;
        }

        Array values = Array.factory(DataType.FLOAT, new int[]{continentalWidth, 2}, array);
        ncFile.write(lonBnds, values);
    }

    private void writeLatBnds(NetcdfFileWriter ncFile, int continentalStartY, int continentalHeight) throws IOException, InvalidRangeException {
        float halfPixelSize = (float) (180.0 / numRowsGlobal);

        Variable latBnds = ncFile.findVariable("lat_bounds");
        float[] array = new float[continentalHeight * 2];
        for (int y = 0; y < continentalHeight; y++) {
            array[2 * y] = (float) crsGrid.getCenterLat(y + continentalStartY) - halfPixelSize;
            array[2 * y + 1] = (float) crsGrid.getCenterLat(y + continentalStartY) + halfPixelSize;
        }

        Array values = Array.factory(DataType.FLOAT, new int[]{continentalHeight, 2}, array);
        ncFile.write(latBnds, values);
    }

    private void writeLat(NetcdfFileWriter ncFile, int continentalStartY, int continentalHeight) throws IOException, InvalidRangeException {
        Variable lat = ncFile.findVariable("lat");
        float[] array = new float[continentalHeight];
        for (int y = 0; y < continentalHeight; y++) {
            array[y] = (float) crsGrid.getCenterLat(y + continentalStartY);
        }
        Array values = Array.factory(DataType.FLOAT, new int[]{continentalHeight}, array);
        ncFile.write(lat, values);
    }

    private void writeLon(NetcdfFileWriter ncFile, int continentalStartX, int continentalWidth) throws IOException, InvalidRangeException {
        Variable lon = ncFile.findVariable("lon");
        float[] array = new float[continentalWidth];
        for (int x = 0; x < continentalWidth; x++) {
            array[x] = (float) crsGrid.getCenterLatLon(x + continentalStartX)[1];
        }
        Array values = Array.factory(DataType.FLOAT, new int[]{continentalWidth}, array);
        ncFile.write(lon, values);
    }

    /**
     * Returns the start x for the given tile, where <code>targetWidth * targetHeight</code> many values are written to
     * the target raster.
     *
     * @param key The mapper key.
     * @return The start x.
     */
    protected int getX(long key) {
        int rowIndex = crsGrid.getRowIndex(key);
        long firstBinIndex = crsGrid.getFirstBinIndex(rowIndex);
        return (int) (key - firstBinIndex);
    }

    /**
     * Returns the start y for the given tile, where <code>targetWidth * targetHeight</code> many values are written to
     * the target raster.
     *
     * @param key The mapper key.
     * @return The start y.
     */
    protected int getY(long key) {
        return crsGrid.getRowIndex(key);
    }


}
