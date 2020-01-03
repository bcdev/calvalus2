package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.hadoop.RasterStackWritable;
import com.bc.calvalus.processing.l3.HadoopBinManager;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.binning.support.CrsGrid;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
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
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FirePixelReducer extends Reducer<LongWritable, RasterStackWritable, NullWritable, NullWritable> {

    private Logger LOG = CalvalusLogger.getLogger();

    protected String outputFilename;
    protected NetcdfFileWriter outputFile;

    private int numRowsGlobal;
    private Rectangle continentalRectangle;
    private CrsGrid planetaryGrid;
    private Product lcProduct;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        try {
            setupInternal(context.getConfiguration());
        } catch (FactoryException | TransformException | InvalidRangeException e) {
            throw new RuntimeException(e);
        }
    }

    void setupInternal(Configuration conf) throws IOException, FactoryException, TransformException, InvalidRangeException {
        // read parameters
        String dateRanges = conf.get("calvalus.input.dateRanges");
        String lcMap = conf.get("calvalus.aux.lcMapPath");
        String regionWkt = conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY);
        String regionName = conf.get("calvalus.input.regionName");
        String version = conf.get("calvalus.output.version", "v5.1");
        numRowsGlobal = HadoopBinManager.getBinningConfig(conf).getNumRows();
        // set derived parameters
        continentalRectangle = computeContinentalRectangle(regionWkt);
        planetaryGrid = new CrsGrid(numRowsGlobal, "EPSG:4326");
        Matcher m = Pattern.compile(".*\\[.*(....-..-..).*:.*(....-..-..).*\\].*").matcher(dateRanges);
        if (! m.matches()) {
            throw new IllegalArgumentException(dateRanges + " is not a date range");
        }
        String timeCoverageStart = m.group(1);
        String timeCoverageEnd = m.group(2);
        String year = timeCoverageStart.substring(0,4);
        String month = timeCoverageStart.substring(5,7);
        LOG.info(String.format("FirePixelReducer setup %s %s %d %s",
                               timeCoverageStart, regionName, numRowsGlobal, continentalRectangle));
        // read LC Map
        Path lcMapPath = new Path(lcMap);
        File lcProductFile =
                        (! (lcMapPath.getFileSystem(conf) instanceof LocalFileSystem)) ?
                                CalvalusProductIO.copyFileToLocal(lcMapPath, conf) :
                                lcMapPath.toString().startsWith("file:") ?
                                        new File(lcMapPath.toString().substring(5)) :
                                        new File(lcMapPath.toString());
        System.setProperty("snap.dataio.netcdf.metadataElementLimit", "0");
        lcProduct = ProductIO.readProduct(lcProductFile);
        LOG.info(String.format("LC Map read from %s", lcMapPath));
        // write band info and metadata to output
        prepareTargetProduct(regionName, timeCoverageStart, timeCoverageEnd, year, month, version);
        LOG.info(String.format("output file %s prepared", outputFilename));
    }

    @Override
    protected void reduce(LongWritable key, Iterable<RasterStackWritable> values, Context context) throws IOException {
        Iterator<RasterStackWritable> iterator = values.iterator();
        RasterStackWritable rasterStackWritable = iterator.next();

        long binIndex = key.get();
        int binX = getX(binIndex);
        int binY = getY(binIndex);

        short[] baData = (short[]) rasterStackWritable.data[0];
        byte[] clData = (byte[]) rasterStackWritable.data[1];
        byte[] lcData = new byte[rasterStackWritable.width * rasterStackWritable.height];
        // TODO: this only works as long as the resolution of BA and LC is the same. Fine for OLCI.
        lcProduct.getBand("lccs_class").readRasterData(binX, binY, rasterStackWritable.width, rasterStackWritable.height, new ProductData.Byte(lcData));

        // force unburnable pixels to -2
        // suppress LC information of pixels not burned
        for (int i = 0; i < baData.length; i++) {
            // not burnable: force to -2, delete lc and cl
            if (!LcRemapping.isInBurnableLcClass(LcRemapping.remap(lcData[i]))) {
                baData[i] = -2;
                clData[i] = 0;
                lcData[i] = 0;
            }
            // should be covered by "not burnable"
            else if (baData[i] == -32767) {
                baData[i] = -2;
                clData[i] = 0;
                lcData[i] = 0;
            }
            // probably not observed (-1): delete lc, set cl to 0 (was: 1)
            else if (baData[i] < 0) {
                //clData[i] = 1;
                clData[i] = 0;
                lcData[i] = 0;
            }
            // not burned (0): delete lc, make cl consistent with observed
            else if (baData[i] == 0) {
                lcData[i] = 0;
                if (clData[i] == 0) {
                    clData[i] = 1;
                }
            }
            // make cl consistent with burned observed pixel
            else if (clData[i] == 0) {
                clData[i] = 1;
            }
            // burned (>=1): keep lc and cl
        }

        try {
            writeShortChunk(binX - continentalRectangle.x, binY - continentalRectangle.y, outputFile, "JD", baData, rasterStackWritable.width, rasterStackWritable.height);
            writeByteChunk(binX - continentalRectangle.x, binY - continentalRectangle.y, outputFile, "CL", clData, rasterStackWritable.width, rasterStackWritable.height);
            writeUByteChunk(binX - continentalRectangle.x, binY - continentalRectangle.y, outputFile, "LC", lcData, rasterStackWritable.width, rasterStackWritable.height);
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
        LOG.info(String.format("chunk %s written to output", new Rectangle(binX, binY, rasterStackWritable.width, rasterStackWritable.height)));
    }

    @Override
    protected void cleanup(Reducer.Context context) throws IOException {
        String outputDir = context.getConfiguration().get("calvalus.output.dir");

        closeNcFile();
        LOG.info(String.format("writing %s finished", outputFilename));

        File fileLocation = new File(outputFilename);
        Path path = new Path(outputDir + "/" + outputFilename);
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        if (!fs.exists(path)) {
            FileUtil.copy(fileLocation, fs, path, false, context.getConfiguration());
            LOG.info(String.format("output file %s archived in %s", outputFilename, outputDir));
        } else {
            LOG.warning(String.format("output file %s not archived in %s, file exists", outputFilename, outputDir));
        }
    }

    void closeNcFile() throws IOException {
        outputFile.close();
    }

    private Rectangle computeContinentalRectangle(String regionWkt) throws FactoryException, TransformException {
        Geometry continentalGeometry = GeometryUtils.createGeometry(regionWkt);
        Product dummyProduct = new Product("dummy", "dummy", numRowsGlobal * 2, numRowsGlobal);
        dummyProduct.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, numRowsGlobal * 2, numRowsGlobal, -180, 90, 360.0 / (numRowsGlobal * 2), 180.0 / numRowsGlobal, 0.0, 0.0));
        return SubsetOp.computePixelRegion(dummyProduct, continentalGeometry, 0);
    }

    private void prepareTargetProduct(String regionName, String timeCoverageStart, String timeCoverageEnd, String year, String month, String version) throws IOException, InvalidRangeException {
        outputFilename = String.format("%s%02d01-C3S-L3S_FIRE-BA-OLCI-%s-f%s.nc",
                                       year, Integer.parseInt(month), regionName, version);
        outputFile = new FirePixelNcFactory().
                createNcFile(outputFilename, version, timeCoverageStart, timeCoverageEnd, numRowsGlobal, continentalRectangle);

        writeLon(outputFile, continentalRectangle.x, continentalRectangle.width);
        writeLat(outputFile, continentalRectangle.y, continentalRectangle.height);

        writeLonBnds(outputFile, continentalRectangle.x, continentalRectangle.width);
        writeLatBnds(outputFile, continentalRectangle.y, continentalRectangle.height);

        writeTime(outputFile, timeCoverageStart);
        writeTimeBnds(outputFile, timeCoverageStart, timeCoverageEnd);

        prepareJdLayer("JD", (short)-2, outputFile);
        prepareByteLayer("CL", (byte)0, outputFile);
        prepareUByteLayer("LC", (byte)0, outputFile);
    }

    protected void writeShortChunk(int x, int y, NetcdfFileWriter ncFile, String varName, short[] data, int width, int height) throws IOException, InvalidRangeException {
        LOG.fine(String.format("Writing data: x=%d, y=%d, %d*%d into variable %s", x, y, width, height, varName));

        Variable variable = ncFile.findVariable(varName);
        Array values = Array.factory(DataType.SHORT, new int[]{1, height, width}, data);
        ncFile.write(variable, new int[]{0, y, x}, values);
    }

    protected void writeByteChunk(int x, int y, NetcdfFileWriter ncFile, String varName, byte[] data, int width, int height) throws IOException, InvalidRangeException {
        LOG.fine(String.format("Writing data: x=%d, y=%d, %d*%d into variable %s", x, y, width, height, varName));

        Variable variable = ncFile.findVariable(varName);
        Array values = Array.factory(DataType.BYTE, new int[]{1, height, width}, data);
        ncFile.write(variable, new int[]{0, y, x}, values);
    }

    protected void writeUByteChunk(int x, int y, NetcdfFileWriter ncFile, String varName, byte[] data, int width, int height) throws IOException, InvalidRangeException {
        LOG.fine(String.format("Writing data: x=%d, y=%d, %d*%d into variable %s", x, y, width, height, varName));

        Variable variable = ncFile.findVariable(varName);
        Array values = Array.factory(DataType.UBYTE, new int[]{1, height, width}, data);
        ncFile.write(variable, new int[]{0, y, x}, values);
    }

    private void prepareJdLayer(String varName, short fillValue, NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Band lcBand = lcProduct.getBand("lccs_class");
        short[] data = new short[1200*1200];
        byte[] lcData = new byte[1200*1200];
        for (int y=0; y<continentalRectangle.height; y+=1200) {
            int h=Math.min(continentalRectangle.height-y, 1200);
            for (int x=0; x<continentalRectangle.width; x+=1200) {
                int w=Math.min(continentalRectangle.width-x, 1200);
                Arrays.fill(data, fillValue);
                // TODO: this only works as long as the resolution of BA and LC is the same. Fine for OLCI.
                lcBand.readRasterData(continentalRectangle.x+x, continentalRectangle.y+y, w, h, new ProductData.Byte(lcData));
                for (int i=0; i<h*w; ++i) {
                    if (LcRemapping.isInBurnableLcClass(LcRemapping.remap(lcData[i]))) {
                        data[i] = -1;
                    }
                }
                writeShortChunk(x, y, ncFile, varName, data, w, h);
            }
        }
    }

    private void prepareByteLayer(String varName, byte fillValue, NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        byte[] data = new byte[1200*1200];
        Arrays.fill(data, fillValue);
        for (int y=0; y<continentalRectangle.height; y+=1200) {
            int h=Math.min(continentalRectangle.height-y, 1200);
            for (int x=0; x<continentalRectangle.width; x+=1200) {
                int w=Math.min(continentalRectangle.width-x, 1200);
                writeByteChunk(x, y, ncFile, varName, data, w, h);
            }
        }
    }

    private void prepareUByteLayer(String varName, byte fillValue, NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        byte[] data = new byte[1200*1200];
        Arrays.fill(data, fillValue);
        for (int y=0; y<continentalRectangle.height; y+=1200) {
            int h=Math.min(continentalRectangle.height-y, 1200);
            for (int x=0; x<continentalRectangle.width; x+=1200) {
                int w=Math.min(continentalRectangle.width-x, 1200);
                writeUByteChunk(x, y, ncFile, varName, data, w, h);
            }
        }
    }


//    protected void writeUByteChunk(int x, int y, NetcdfFileWriter outputFile, String varName, byte[] data, int width, int height) throws IOException, InvalidRangeException {
//        CalvalusLogger.getLogger().info(String.format("Writing data: x=%d, y=%d, %d*%d into variable %s", x, y, width, height, varName));
//
//        Variable variable = outputFile.findVariable(varName);
//        Array values = Array.factory(DataType.UBYTE, new int[]{1, width, height}, data);
//        outputFile.write(variable, new int[]{0, y, x}, values);
//    }

    private static void writeTimeBnds(NetcdfFileWriter ncFile, String timeCoverageStart, String timeCoverageEnd) throws IOException, InvalidRangeException {
        double firstDayAsJD = getDayAsJD(timeCoverageStart);
        double lastDayAsJD = getDayAsJD(timeCoverageEnd);
        Variable timeBnds = ncFile.findVariable("time_bounds");
        Array values = Array.factory(DataType.DOUBLE, new int[] {1, 2}, new double[] { firstDayAsJD, lastDayAsJD });
        ncFile.write(timeBnds, values);
    }

    private static void writeTime(NetcdfFileWriter ncFile, String timeCoverageStart) throws IOException, InvalidRangeException {
        double firstDayAsJD = getDayAsJD(timeCoverageStart);
        Variable time = ncFile.findVariable("time");
        Array values = Array.factory(DataType.DOUBLE, new int[] {1}, new double[] { firstDayAsJD });
        ncFile.write(time, values);
    }

    private static double getDayAsJD(String date) {
        String year = date.substring(0,4);
        String month = date.substring(5,7);
        String day = date.substring(8,10);
        LocalDate current = Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).atDay(Integer.parseInt(day));
        LocalDate epoch = Year.of(1970).atMonth(1).atDay(1);
        return ChronoUnit.DAYS.between(epoch, current);
    }

    private void writeLonBnds(NetcdfFileWriter ncFile, int continentalStartX, int continentalWidth) throws IOException, InvalidRangeException {
        float halfPixelSize = (float) (360.0 / (numRowsGlobal * 2));

        Variable lonBnds = ncFile.findVariable("lon_bounds");
        double[] array = new double[continentalWidth * 2];

        for (int x = 0; x < continentalWidth; x++) {
            array[2 * x] = planetaryGrid.getCenterLatLon(x + continentalStartX)[1] - halfPixelSize;
            array[2 * x + 1] = planetaryGrid.getCenterLatLon(x + continentalStartX)[1] + halfPixelSize;
        }

        Array values = Array.factory(DataType.DOUBLE, new int[]{continentalWidth, 2}, array);
        ncFile.write(lonBnds, values);
    }

    private void writeLatBnds(NetcdfFileWriter ncFile, int continentalStartY, int continentalHeight) throws IOException, InvalidRangeException {
        float halfPixelSize = (float) (180.0 / numRowsGlobal);

        Variable latBnds = ncFile.findVariable("lat_bounds");
        double[] array = new double[continentalHeight * 2];
        for (int y = 0; y < continentalHeight; y++) {
            array[2 * y] = planetaryGrid.getCenterLat(y + continentalStartY) - halfPixelSize;
            array[2 * y + 1] = planetaryGrid.getCenterLat(y + continentalStartY) + halfPixelSize;
        }

        Array values = Array.factory(DataType.DOUBLE, new int[]{continentalHeight, 2}, array);
        ncFile.write(latBnds, values);
    }

    private void writeLat(NetcdfFileWriter ncFile, int continentalStartY, int continentalHeight) throws IOException, InvalidRangeException {
        Variable lat = ncFile.findVariable("lat");
        double[] array = new double[continentalHeight];
        for (int y = 0; y < continentalHeight; y++) {
            array[y] = planetaryGrid.getCenterLat(y + continentalStartY);
        }
        Array values = Array.factory(DataType.DOUBLE, new int[]{continentalHeight}, array);
        ncFile.write(lat, values);
    }

    private void writeLon(NetcdfFileWriter ncFile, int continentalStartX, int continentalWidth) throws IOException, InvalidRangeException {
        Variable lon = ncFile.findVariable("lon");
        double[] array = new double[continentalWidth];
        for (int x = 0; x < continentalWidth; x++) {
            array[x] = planetaryGrid.getCenterLatLon(x + continentalStartX)[1];
        }
        Array values = Array.factory(DataType.DOUBLE, new int[]{continentalWidth}, array);
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
        int rowIndex = planetaryGrid.getRowIndex(key);
        long firstBinIndex = planetaryGrid.getFirstBinIndex(rowIndex);
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
        return planetaryGrid.getRowIndex(key);
    }


}
