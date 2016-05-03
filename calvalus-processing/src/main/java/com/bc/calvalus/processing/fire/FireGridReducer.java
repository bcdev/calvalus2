package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.ceres.core.Assert;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.Year;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author thomas
 */
public class FireGridReducer extends Reducer<Text, GridCell, NullWritable, NullWritable> {

    private static final int SCENE_RASTER_WIDTH = 1440;
    private static final int SCENE_RASTER_HEIGHT = 720;
    //    private Product resultFirstHalf;
//    private Product resultSecondHalf;
//    private final List<int[]> writtenChunksFirstHalf = new ArrayList<>();
//    private final List<int[]> writtenChunksSecondHalf = new ArrayList<>();
    private NetcdfFileWriter ncFirst;
    private NetcdfFileWriter ncSecond;
    private String firstHalfFile;
    private String secondHalfFile;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        prepareTargetProducts(context);
    }

    @Override
    protected void reduce(Text key, Iterable<GridCell> values, Context context) throws IOException, InterruptedException {
        Iterator<GridCell> iterator = values.iterator();
        GridCell gridCell = iterator.next();

        float[] burnedAreaFirstHalf = gridCell.baFirstHalf;
        float[] burnedAreaSecondHalf = gridCell.baSecondHalf;

        float[] patchNumbersFirstHalf = gridCell.patchNumberFirstHalf;
        float[] patchNumbersSecondHalf = gridCell.patchNumberSecondHalf;

        float[] errorsFirstHalf = gridCell.errorsFirstHalf;
        float[] errorsSecondHalf = gridCell.errorsSecondHalf;

        List<float[]> baInLcFirstHalf = gridCell.baInLcFirstHalf;
        List<float[]> baInLcSecondHalf = gridCell.baInLcSecondHalf;

        float[] coverageFirstHalf = gridCell.coverageFirstHalf;
        float[] coverageSecondHalf = gridCell.coverageSecondHalf;

        try {
            writeFloatChunk(key.toString(), ncFirst, "burned_area", burnedAreaFirstHalf);
            writeFloatChunk(key.toString(), ncSecond, "burned_area", burnedAreaSecondHalf);

            writeFloatChunk(key.toString(), ncFirst, "standard_error", errorsFirstHalf);
            writeFloatChunk(key.toString(), ncSecond, "standard_error", errorsSecondHalf);

            writeFloatChunk(key.toString(), ncFirst, "observed_area_fraction", coverageFirstHalf);
            writeFloatChunk(key.toString(), ncSecond, "observed_area_fraction", coverageSecondHalf);

            writeFloatChunk(key.toString(), ncFirst, "number_of_patches", patchNumbersFirstHalf);
            writeFloatChunk(key.toString(), ncSecond, "number_of_patches", patchNumbersSecondHalf);

            for (int i = 0; i < baInLcFirstHalf.size(); i++) {
                float[] baInClass = baInLcFirstHalf.get(i);
                writeVegetationChunk(key.toString(), i, ncFirst, baInClass);
            }
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }

//        writeData(key, burnedAreaFirstHalf, patchNumbersFirstHalf, errorsFirstHalf, baInLcFirstHalf, coverageFirstHalf, resultFirstHalf, writtenChunksFirstHalf);
//        writeData(key, burnedAreaSecondHalf, patchNumbersSecondHalf, errorsSecondHalf, baInLcSecondHalf, coverageSecondHalf, resultSecondHalf, writtenChunksSecondHalf);
    }

    private void prepareTargetProducts(Context context) throws IOException {
        String year = context.getConfiguration().get("calvalus.year");
        String month = context.getConfiguration().get("calvalus.month");
        Assert.notNull(year, "calvalus.year");
        Assert.notNull(month, "calvalus.month");

        firstHalfFile = createFilename(year, month, true);
        secondHalfFile = createFilename(year, month, false);

        ncFirst = createNcFile(firstHalfFile);
        ncSecond = createNcFile(secondHalfFile);

//        resultFirstHalf = new Product("target", "type", SCENE_RASTER_WIDTH, SCENE_RASTER_HEIGHT);
//        resultSecondHalf = new Product("target", "type", SCENE_RASTER_WIDTH, SCENE_RASTER_HEIGHT);
//        resultFirstHalf.setProductWriter(new CfNetCdf4WriterPlugIn().createWriterInstance());
//        resultSecondHalf.setProductWriter(new CfNetCdf4WriterPlugIn().createWriterInstance());
//        resultFirstHalf.setFileLocation(new File(createFilename(year, month, true) + "-old.nc"));
//        resultSecondHalf.setFileLocation(new File("./thomas-ba-22.nc"));
//        try {
//            resultFirstHalf.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, SCENE_RASTER_WIDTH, SCENE_RASTER_HEIGHT, -180, 90, 0.25, 0.25, 0.0, 0.0));
//            resultSecondHalf.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, SCENE_RASTER_WIDTH, SCENE_RASTER_HEIGHT, -180, 90, 0.25, 0.25, 0.0, 0.0));
//        } catch (FactoryException | TransformException e) {
//            throw new IOException("Unable to create geo-coding", e);
//        }

        ncFirst.create();
        ncSecond.create();

        try {
            writeLon(ncFirst);
            writeLon(ncSecond);
            writeLat(ncFirst);
            writeLat(ncSecond);

            writeLonBnds(ncFirst);
            writeLonBnds(ncSecond);
            writeLatBnds(ncFirst);
            writeLatBnds(ncSecond);

            writeTime(ncFirst, year, month, true);
            writeTime(ncSecond, year, month, false);

            writeTimeBnds(ncFirst, "2006", "08", true);
            writeTimeBnds(ncSecond, "2006", "08", false);

            writeVegetationClasses(ncFirst);
            writeVegetationClasses(ncSecond);

            writeVegetationClassNames(ncFirst);
            writeVegetationClassNames(ncSecond);

        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }

//        Band burnedAreaFirstHalf = resultFirstHalf.addBand("burned_area", ProductData.TYPE_FLOAT32);
//        burnedAreaFirstHalf.setUnit("m2");
//
//        Band burnedAreaSecondHalf = resultSecondHalf.addBand("burned_area", ProductData.TYPE_FLOAT32);
//        burnedAreaSecondHalf.setUnit("m2");
//
//        Band standardErrorFirstHalf = resultFirstHalf.addBand("standard_error", ProductData.TYPE_FLOAT32);
//        standardErrorFirstHalf.setUnit("m2");
//
//        Band standardErrorSecondHalf = resultSecondHalf.addBand("standard_error", ProductData.TYPE_FLOAT32);
//        standardErrorSecondHalf.setUnit("m2");
//
//        Band coverageFirstHalf = resultFirstHalf.addBand("observed_area_fraction", ProductData.TYPE_FLOAT32);
//        coverageFirstHalf.setUnit("1");
//
//        Band coverageSecondHalf = resultSecondHalf.addBand("observed_area_fraction", ProductData.TYPE_FLOAT32);
//        coverageSecondHalf.setUnit("1");
//
//        resultFirstHalf.addBand("patch_number", ProductData.TYPE_FLOAT32);
//        resultSecondHalf.addBand("patch_number", ProductData.TYPE_FLOAT32);
//
//        for (int lcClass = 1; lcClass <= FireGridMapper.LC_CLASSES_COUNT; lcClass++) {
//            Band band1 = resultFirstHalf.addBand(getBandNameFor(lcClass), ProductData.TYPE_FLOAT32);
//            Band band2 = resultSecondHalf.addBand(getBandNameFor(lcClass), ProductData.TYPE_FLOAT32);
//            band1.setUnit("m2");
//            band2.setUnit("m2");
//        }

//        ProductWriter productWriterFH = resultFirstHalf.getProductWriter();
//        ProductWriter productWriterSH = resultSecondHalf.getProductWriter();
//        productWriterFH.writeProductNodes(resultFirstHalf, resultFirstHalf.getFileLocation());
//        productWriterSH.writeProductNodes(resultSecondHalf, resultSecondHalf.getFileLocation());
    }

    private static void writeFloatChunk(String key, NetcdfFileWriter ncFile, String varName, float[] data) throws IOException, InvalidRangeException {
        int x = getX(key);
        int y = getY(key);
        CalvalusLogger.getLogger().info(String.format("Writing data: x=%d, y=%d, 40*40 (tile %s) into variable %s", x, y, key, varName));

        Variable variable = ncFile.findVariable(varName);
        Array values = Array.factory(DataType.FLOAT, new int[]{1, 40, 40}, data);
        ncFile.write(variable, new int[]{0, y, x}, values);
    }

    private static void writeVegetationChunk(String key, int lcClassIndex, NetcdfFileWriter ncFile, float[] baInClass) throws IOException, InvalidRangeException {
        int x = getX(key);
        int y = getY(key);
        CalvalusLogger.getLogger().info(String.format("Writing raster data: x=%d, y=%d, 40*40 into lc class %d", x, y, lcClassIndex));

        Variable variable = ncFile.findVariable("burned_area_in_vegetation_class");
        Array values = Array.factory(DataType.FLOAT, new int[]{1, 1, 40, 40}, baInClass);
        ncFile.write(variable, new int[]{0, lcClassIndex, y, x}, values);
    }

    private static void writeVegetationClassNames(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable vegetationClass = ncFile.findVariable("vegetation_class_name");
        List<String> names = new ArrayList<>();
        names.add("Cropland, rainfed");
        names.add("Cropland, irrigated or post-flooding");
        names.add("Mosaic cropland (>50%) / natural vegetation (tree, shrub, herbaceous cover) (<50%)");
        names.add("Mosaic natural vegetation (tree, shrub, herbaceous cover) (>50%) / cropland (<50%)");
        names.add("Tree cover, broadleaved, evergreen, closed to open (>15%)");
        names.add("Tree cover, broadleaved, deciduous, closed to open (>15%)");
        names.add("Tree cover, needleleaved, evergreen, closed to open (>15%)");
        names.add("Tree cover, needleleaved, deciduous, closed to open (>15%)");
        names.add("Tree cover, mixed leaf type (broadleaved and needleleaved)");
        names.add("Mosaic tree and shrub (>50%) / herbaceous cover (<50%)");
        names.add("Mosaic herbaceous cover (>50%) / tree and shrub (<50%)");
        names.add("Shrubland");
        names.add("Grassland");
        names.add("Lichens and mosses");
        names.add("Sparse vegetation (tree, shrub, herbaceous cover) (<15%)");
        names.add("Tree cover, flooded, fresh or brakish water");
        names.add("Tree cover, flooded, saline water");
        names.add("Shrub or herbaceous cover, flooded, fresh/saline/brakish water");
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            char[] array = name.toCharArray();
            Array values = Array.factory(DataType.CHAR, new int[]{1, name.length()}, array);
            ncFile.write(vegetationClass, new int[]{i, 0}, values);
        }
    }

    private static void writeVegetationClasses(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable vegetationClass = ncFile.findVariable("vegetation_class");
        int[] array = new int[]{
                10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130,
                140, 150, 160, 170, 180
        };
        Array values = Array.factory(DataType.INT, new int[]{18}, array);
        ncFile.write(vegetationClass, values);
    }

    private static void writeTimeBnds(NetcdfFileWriter ncFile, String year, String month, boolean firstHalf) throws IOException, InvalidRangeException {
        Variable timeBnds = ncFile.findVariable("time_bnds");
        double centralTime = getTimeValue(year, month, firstHalf);
        int lengthOfMonth = Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).lengthOfMonth();
        float[] array = new float[]{
                (float) (centralTime - (firstHalf ? 7 : 6)),
                (float) (centralTime + (firstHalf ? 8 : lengthOfMonth - 22))
        };
        Array values = Array.factory(DataType.FLOAT, new int[]{1, 2}, array);
        ncFile.write(timeBnds, values);
    }

    private static void writeTime(NetcdfFileWriter ncFile, String year, String month, boolean firstHalf) throws IOException, InvalidRangeException {
        Variable time = ncFile.findVariable("time");
        double[] array = new double[]{getTimeValue(year, month, firstHalf)};
        Array values = Array.factory(DataType.DOUBLE, new int[]{1}, array);
        ncFile.write(time, values);
    }

    private static double getTimeValue(String year, String month, boolean firstHalf) {
        int day = firstHalf ? 7 : 22;
        LocalDate current = Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).atDay(day);
        LocalDate epoch = Year.of(1970).atMonth(1).atDay(1);
        return ChronoUnit.DAYS.between(epoch, current);
    }

    private static void writeLonBnds(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable lonBnds = ncFile.findVariable("lon_bnds");
        float[] array = new float[SCENE_RASTER_WIDTH * 2];
        array[0] = -180F;
        for (int x = 1; x < SCENE_RASTER_WIDTH * 2; x++) {
            if (x % 2 == 0) {
                array[x] = array[x - 1];
            } else {
                array[x] = array[x - 1] + 0.25F;
            }
        }
        Array values = Array.factory(DataType.FLOAT, new int[]{SCENE_RASTER_WIDTH, 2}, array);
        ncFile.write(lonBnds, values);
    }

    private static void writeLatBnds(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable latBnds = ncFile.findVariable("lat_bnds");
        float[] array = new float[SCENE_RASTER_HEIGHT * 2];
        array[0] = 90F;
        for (int y = 1; y < SCENE_RASTER_HEIGHT * 2; y++) {
            if (y % 2 == 0) {
                array[y] = array[y - 1];
            } else {
                array[y] = array[y - 1] - 0.25F;
            }
        }
        Array values = Array.factory(DataType.FLOAT, new int[]{SCENE_RASTER_HEIGHT, 2}, array);
        ncFile.write(latBnds, values);
    }

    private static void writeLat(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable lat = ncFile.findVariable("lat");
        float[] array = new float[SCENE_RASTER_HEIGHT];
        array[0] = 89.875F;
        for (int x = 1; x < SCENE_RASTER_HEIGHT; x++) {
            array[x] = array[x - 1] - 0.25F;
        }
        Array values = Array.factory(DataType.FLOAT, new int[]{SCENE_RASTER_HEIGHT}, array);
        ncFile.write(lat, values);
    }

    private static void writeLon(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable lon = ncFile.findVariable("lon");
        float[] array = new float[SCENE_RASTER_WIDTH];
        array[0] = -179.875F;
        for (int x = 1; x < SCENE_RASTER_WIDTH; x++) {
            array[x] = array[x - 1] + 0.25F;
        }
        Array values = Array.factory(DataType.FLOAT, new int[]{SCENE_RASTER_WIDTH}, array);
        ncFile.write(lon, values);
    }

    private void writeData(Text key, float[] burnedArea, float[] patchNumbers, float[] errors, List<float[]> baInLc, float[] coverage, Product product, List<int[]> writtenChunks) throws IOException {
        int x = getX(key.toString());
        int y = getY(key.toString());
        CalvalusLogger.getLogger().info(String.format("Writing raster data: x=%d, y=%d, 40*40", x, y));
        ProductWriter productWriter = product.getProductWriter();
        productWriter.writeBandRasterData(product.getBand("burned_area"), x, y, 40, 40, new ProductData.Float(burnedArea), ProgressMonitor.NULL);
        productWriter.writeBandRasterData(product.getBand("patch_number"), x, y, 40, 40, new ProductData.Float(patchNumbers), ProgressMonitor.NULL);
        productWriter.writeBandRasterData(product.getBand("standard_error"), x, y, 40, 40, new ProductData.Float(errors), ProgressMonitor.NULL);
        productWriter.writeBandRasterData(product.getBand("observed_area_fraction"), x, y, 40, 40, new ProductData.Float(coverage), ProgressMonitor.NULL);
        for (int lcClass = 1; lcClass <= FireGridMapper.LC_CLASSES_COUNT; lcClass++) {
            productWriter.writeBandRasterData(product.getBand(getBandNameFor(lcClass)), x, y, 40, 40, new ProductData.Float(baInLc.get(lcClass - 1)), ProgressMonitor.NULL);
        }
        if (!alreadyWritten(x, y, writtenChunks)) {
            writtenChunks.add(new int[]{x, y});
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
//        fillBand(resultFirstHalf.getBand("burned_area"), resultFirstHalf.getProductWriter(), writtenChunksFirstHalf, createFloatFillData());
//        fillBand(resultSecondHalf.getBand("burned_area"), resultSecondHalf.getProductWriter(), writtenChunksSecondHalf, createFloatFillData());
//        fillBand(resultFirstHalf.getBand("patch_number"), resultFirstHalf.getProductWriter(), writtenChunksFirstHalf, createFloatFillData());
//        fillBand(resultSecondHalf.getBand("patch_number"), resultSecondHalf.getProductWriter(), writtenChunksSecondHalf, createFloatFillData());
//        fillBand(resultFirstHalf.getBand("standard_error"), resultFirstHalf.getProductWriter(), writtenChunksFirstHalf, createFloatFillData());
//        fillBand(resultSecondHalf.getBand("standard_error"), resultSecondHalf.getProductWriter(), writtenChunksSecondHalf, createFloatFillData());
//        fillBand(resultFirstHalf.getBand("observed_area_fraction"), resultFirstHalf.getProductWriter(), writtenChunksFirstHalf, createFloatFillData());
//        fillBand(resultSecondHalf.getBand("observed_area_fraction"), resultSecondHalf.getProductWriter(), writtenChunksSecondHalf, createFloatFillData());
//        for (int lcClass = 1; lcClass <= FireGridMapper.LC_CLASSES_COUNT; lcClass++) {
//            fillBand(resultFirstHalf.getBand(getBandNameFor(lcClass)), resultFirstHalf.getProductWriter(), writtenChunksFirstHalf, createFloatFillData());
//            fillBand(resultSecondHalf.getBand(getBandNameFor(lcClass)), resultSecondHalf.getProductWriter(), writtenChunksFirstHalf, createFloatFillData());
//        }
//        write(context.getConfiguration(), resultFirstHalf, "hdfs://calvalus/calvalus/home/thomas/thomas-ba-2008-06-07.nc");
//        write(context.getConfiguration(), resultSecondHalf, "hdfs://calvalus/calvalus/home/thomas/thomas-ba-2008-06-22.nc");

        ncFirst.close();
        ncSecond.close();

        File fileLocation = new File("./" + firstHalfFile);
        File fileLocation2 = new File("./" + secondHalfFile);
        Path path = new Path("hdfs://calvalus/calvalus/home/thomas/" + firstHalfFile);
        Path path2 = new Path("hdfs://calvalus/calvalus/home/thomas/" + secondHalfFile);
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        FileUtil.copy(fileLocation, fs, path, false, context.getConfiguration());
        FileUtil.copy(fileLocation2, fs, path2, false, context.getConfiguration());
    }

    private static NetcdfFileWriter createNcFile(String filename) throws IOException {
        NetcdfFileWriter ncFile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3c64, filename);

        ncFile.addDimension(null, "vegetation_class", 18);
        ncFile.addDimension(null, "lat", 720);
        ncFile.addDimension(null, "lon", 1440);
        ncFile.addDimension(null, "nv", 2);
        ncFile.addDimension(null, "strlen", 150);
        ncFile.addUnlimitedDimension("time");

        Variable latVar = ncFile.addVariable(null, "lat", DataType.FLOAT, "lat");
        latVar.addAttribute(new Attribute("units", "degree_north"));
        latVar.addAttribute(new Attribute("standard_name", "latitude"));
        latVar.addAttribute(new Attribute("long_name", "latitude"));
        latVar.addAttribute(new Attribute("bounds", "lat_bnds"));
        ncFile.addVariable(null, "lat_bnds", DataType.FLOAT, "lat nv");
        Variable lonVar = ncFile.addVariable(null, "lon", DataType.FLOAT, "lon");
        lonVar.addAttribute(new Attribute("units", "degree_east"));
        lonVar.addAttribute(new Attribute("standard_name", "longitude"));
        lonVar.addAttribute(new Attribute("long_name", "longitude"));
        lonVar.addAttribute(new Attribute("bounds", "lon_bnds"));
        ncFile.addVariable(null, "lon_bnds", DataType.FLOAT, "lon nv");
        Variable timeVar = ncFile.addVariable(null, "time", DataType.DOUBLE, "time");
        timeVar.addAttribute(new Attribute("units", "days since 1970-01-01 00:00:00"));
        timeVar.addAttribute(new Attribute("standard_name", "time"));
        timeVar.addAttribute(new Attribute("long_name", "time"));
        timeVar.addAttribute(new Attribute("bounds", "time_bnds"));
        timeVar.addAttribute(new Attribute("calendar", "standard"));
        ncFile.addVariable(null, "time_bnds", DataType.FLOAT, "time nv");
        Variable vegetationClassVar = ncFile.addVariable(null, "vegetation_class", DataType.INT, "vegetation_class");
        vegetationClassVar.addAttribute(new Attribute("units", "1"));
        vegetationClassVar.addAttribute(new Attribute("long_name", "vegetation class"));
        Variable vegetationClassNameVar = ncFile.addVariable(null, "vegetation_class_name", DataType.CHAR, "vegetation_class strlen");
        vegetationClassNameVar.addAttribute(new Attribute("units", "1"));
        vegetationClassNameVar.addAttribute(new Attribute("long_name", "vegetation class name"));
        Variable burnedAreaVar = ncFile.addVariable(null, "burned_area", DataType.FLOAT, "time lat lon");
        burnedAreaVar.addAttribute(new Attribute("units", "m2"));
        burnedAreaVar.addAttribute(new Attribute("standard_name", "burned_area"));
        burnedAreaVar.addAttribute(new Attribute("long_name", "total burned_area"));
        burnedAreaVar.addAttribute(new Attribute("cell_methods", "time: sum"));
        Variable standardErrorVar = ncFile.addVariable(null, "standard_error", DataType.FLOAT, "time lat lon");
        standardErrorVar.addAttribute(new Attribute("units", "m2"));
        standardErrorVar.addAttribute(new Attribute("long_name", "standard error of the estimation of burned area"));
        Variable observedAreaFractionVar = ncFile.addVariable(null, "observed_area_fraction", DataType.FLOAT, "time lat lon");
        observedAreaFractionVar.addAttribute(new Attribute("units", "1"));
        observedAreaFractionVar.addAttribute(new Attribute("long_name", "fraction of observed area"));
        observedAreaFractionVar.addAttribute(new Attribute("comment", "The fraction of observed area is 1 minus the area fraction of unsuitable/not observable pixels in a given grid. The latter refers to the area where it was not possible to obtain observational burned area information for the whole time interval because of cloud cover, haze or pixels that fell below the quality thresholds of the algorithm."));
        Variable numberOfPatchesVar = ncFile.addVariable(null, "number_of_patches", DataType.FLOAT, "time lat lon");
        numberOfPatchesVar.addAttribute(new Attribute("units", "1"));
        numberOfPatchesVar.addAttribute(new Attribute("long_name", "number of burn patches"));
        numberOfPatchesVar.addAttribute(new Attribute("comment", "Number of contiguous groups of burned pixels."));
        Variable burnedAreaInVegClassVar = ncFile.addVariable(null, "burned_area_in_vegetation_class", DataType.FLOAT, "time vegetation_class lat lon");
        burnedAreaInVegClassVar.addAttribute(new Attribute("units", "m2"));
        burnedAreaInVegClassVar.addAttribute(new Attribute("long_name", "burned area in vegetation class"));
        burnedAreaInVegClassVar.addAttribute(new Attribute("cell_methods", "time: sum"));
        burnedAreaInVegClassVar.addAttribute(new Attribute("comment", "Burned area by land cover classes; land cover classes are from CCI Land Cover, http://www.esa-landcover-cci.org/"));

        ncFile.addGroupAttribute(null, new Attribute("Conventions", "CF-1.6"));
        ncFile.addGroupAttribute(null, new Attribute("title", "Fire_cci Gridded MERIS Burned Area product"));
        ncFile.addGroupAttribute(null, new Attribute("version", "MERIS BA Algorithm v4"));
        ncFile.addGroupAttribute(null, new Attribute("source", "Digital image processing of MERIS and MODIS hotspots data"));
        ncFile.addGroupAttribute(null, new Attribute("institution", "University of Alcala’s consortium for ESA CCI program"));
        ncFile.addGroupAttribute(null, new Attribute("project", "ESA Climate Change Initiative, ECV Fire Disturbance (Fire_cci)"));
        ncFile.addGroupAttribute(null, new Attribute("references", "see http://dx.doi.org/10.1016/j.rse.2015.03.011"));
        ncFile.addGroupAttribute(null, new Attribute("acknowledgment", "ESA CCI program"));
        ncFile.addGroupAttribute(null, new Attribute("contact", "Emilio Chuvieco. emilio.chuvieco@uah.es"));
        ncFile.addGroupAttribute(null, new Attribute("history", ""));
        return ncFile;
    }

    static String createFilename(String year, String month, boolean firstHalf) {
        return String.format("%s%s%s-ESACCI-L4_FIRE-BA-MERIS-fv04.0.nc", year, month, firstHalf ? "07" : "22");
    }

    private static String getBandNameFor(int lcClass) {
        return String.format("burned_area_in_vegetation_class_vegetation_class%d", lcClass);
    }

    private static ProductData.Int createIntFillData() {
        int[] array = new int[40 * 40];
        Arrays.fill(array, 0);
        return new ProductData.Int(array);
    }

    private static ProductData.Float createFloatFillData() {
        float[] array = new float[40 * 40];
        Arrays.fill(array, 0.0F);
        return new ProductData.Float(array);
    }

    private void write(Configuration configuration, Product product, String pathString) throws IOException {
        product.closeIO();
        File fileLocation = product.getFileLocation();
        Path path = new Path(pathString);
        FileSystem fs = path.getFileSystem(configuration);
        fs.delete(path, true);
        FileUtil.copy(fileLocation, fs, path, false, configuration);
    }

    private static void fillBand(Band band, ProductWriter productWriter, List<int[]> writtenChunks, ProductData fillData) throws IOException {
        // the netcdf4-writer expects the band to be fully populated with data
        // also, it can write data only once
        // --> we have to fill remaining pixels here
        for (int y = 0; y < SCENE_RASTER_HEIGHT; y += 40) {
            for (int x = 0; x < SCENE_RASTER_WIDTH; x += 40) {
                if (!alreadyWritten(x, y, writtenChunks)) {
                    CalvalusLogger.getLogger().info("Filling chunk at: x=" + x + "; y=" + y);
                    productWriter.writeBandRasterData(band, x, y, 40, 40, fillData, ProgressMonitor.NULL);
                }
            }
        }
    }

    private static boolean alreadyWritten(int x, int y, List<int[]> writtenChunks) {
        for (int[] item : writtenChunks) {
            if (Arrays.equals(item, new int[]{x, y})) {
                return true;
            }
        }
        return false;
    }

    private static int getX(String key) {
        int x = Integer.parseInt(key.substring(12));
        return x * 40;
    }

    private static int getY(String key) {
        int y = Integer.parseInt(key.substring(9, 11));
        return y * 40;
    }

    public static void main(String[] args) throws IOException, InvalidRangeException {
        NetcdfFileWriter ncFirst = createNcFile(createFilename("2006", "08", true));
        NetcdfFileWriter ncSecond = createNcFile(createFilename("2006", "08", false));

        ncFirst.create();
        ncSecond.create();

        writeLon(ncFirst);
        writeLon(ncSecond);
        writeLat(ncFirst);
        writeLat(ncSecond);

        writeLonBnds(ncFirst);
        writeLonBnds(ncSecond);
        writeLatBnds(ncFirst);
        writeLatBnds(ncSecond);

        writeTime(ncFirst, "2006", "08", true);
        writeTime(ncSecond, "2006", "08", false);

        writeTimeBnds(ncFirst, "2006", "08", true);
        writeTimeBnds(ncSecond, "2006", "08", false);

        writeVegetationClasses(ncFirst);
        writeVegetationClasses(ncSecond);

        writeVegetationClassNames(ncFirst);
        writeVegetationClassNames(ncSecond);

        try {
            float[] data = new float[40 * 40];
            for (int i = 0; i < data.length; i++) {
                data[i] = i;
            }
            float[] data2 = new float[40 * 40];
            for (int i = 0; i < data2.length; i++) {
                data2[i] = 10000 + i;
            }
            writeFloatChunk(String.format("%d-%02d-%s", 2008, 8, "v03h12"), ncFirst, "burned_area", data);
            writeFloatChunk(String.format("%d-%02d-%s", 2008, 8, "v03h12"), ncSecond, "burned_area", data2);
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }

        ncFirst.close();
        ncSecond.close();
    }

}
