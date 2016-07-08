package com.bc.calvalus.processing.fire.format.grid;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.ceres.core.Assert;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.Year;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author thomas
 */
public class GridReducer extends Reducer<Text, GridCell, NullWritable, NullWritable> {

    private static final int SCENE_RASTER_WIDTH = 1440;
    private static final int SCENE_RASTER_HEIGHT = 720;

    private NetcdfFileWriter ncFirst;
    private NetcdfFileWriter ncSecond;

    private String firstHalfFile;
    private String secondHalfFile;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        prepareTargetProducts(context);

        String year = context.getConfiguration().get("calvalus.year");
        String month = context.getConfiguration().get("calvalus.month");
        String basePath = "hdfs://calvalus/calvalus/projects/fire/aux/psd-grid-for-oaf/";
        String firstHalfFilename = year + month + "07-ESACCI-L4_FIRE-BA-MERIS-fv04.1.nc";
        String secondHalfFilename = year + month + "22-ESACCI-L4_FIRE-BA-MERIS-fv04.1.nc";
        Path firstHalfGridPath = new Path(basePath, firstHalfFilename);
        Path secondHalfGridPath = new Path(basePath, secondHalfFilename);
        File firstHalfGridFile = CalvalusProductIO.copyFileToLocal(firstHalfGridPath, context.getConfiguration());
        File secondHalfGridFile = CalvalusProductIO.copyFileToLocal(secondHalfGridPath, context.getConfiguration());

        Product firstHalfGridProduct = ProductIO.readProduct(firstHalfGridFile);
        Product secondHalfGridProduct = ProductIO.readProduct(secondHalfGridFile);

        float[] buffer = new float[40 * 40];

        try {
            for (int y = 0; y < 18; y++) {
                for (int x = 0; x < 36; x++) {
                    firstHalfGridProduct.getBand("observed_area_fraction").readPixels(x * 40, y * 40, 40, 40, buffer);
                    writeFloatChunk(x * 40, y * 40, ncFirst, "observed_area_fraction", buffer);
                    secondHalfGridProduct.getBand("observed_area_fraction").readPixels(x * 40, y * 40, 40, 40, buffer);
                    writeFloatChunk(x * 40, y * 40, ncSecond, "observed_area_fraction", buffer);
                }
            }
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }

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


        try {
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncFirst, "burned_area", burnedAreaFirstHalf);
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncSecond, "burned_area", burnedAreaSecondHalf);

            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncFirst, "standard_error", errorsFirstHalf);
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncSecond, "standard_error", errorsSecondHalf);

            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncFirst, "number_of_patches", patchNumbersFirstHalf);
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncSecond, "number_of_patches", patchNumbersSecondHalf);


            for (int i = 0; i < baInLcFirstHalf.size(); i++) {
                float[] baInClass = baInLcFirstHalf.get(i);
                writeVegetationChunk(key.toString(), i, ncFirst, baInClass);
            }
            for (int i = 0; i < baInLcSecondHalf.size(); i++) {
                float[] baInClass = baInLcSecondHalf.get(i);
                writeVegetationChunk(key.toString(), i, ncSecond, baInClass);
            }
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String outputDir = context.getConfiguration().get("calvalus.output.dir");

        ncFirst.close();
        ncSecond.close();

        File fileLocation = new File("./" + firstHalfFile);
        File fileLocation2 = new File("./" + secondHalfFile);
        Path path = new Path(outputDir + "/" + firstHalfFile);
        Path path2 = new Path(outputDir + "/" + secondHalfFile);
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        FileUtil.copy(fileLocation, fs, path, false, context.getConfiguration());
        FileUtil.copy(fileLocation2, fs, path2, false, context.getConfiguration());
    }

    private void prepareTargetProducts(Context context) throws IOException {
        String year = context.getConfiguration().get("calvalus.year");
        String month = context.getConfiguration().get("calvalus.month");
        String version = context.getConfiguration().get("calvalus.baversion", "v4.1");
        Assert.notNull(year, "calvalus.year");
        Assert.notNull(month, "calvalus.month");

        int lastDayOfMonth = Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).lengthOfMonth();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneId.systemDefault());
        String timeCoverageStartFirstHalf = dtf.format(LocalDate.of(Integer.parseInt(year), Integer.parseInt(month), 1).atTime(0, 0, 0));
        String timeCoverageEndFirstHalf = dtf.format(LocalDate.of(Integer.parseInt(year), Integer.parseInt(month), 15).atTime(23, 59, 59));
        String timeCoverageStartSecondHalf = dtf.format(LocalDate.of(Integer.parseInt(year), Integer.parseInt(month), 16).atTime(0, 0, 0));
        String timeCoverageEndSecondHalf = dtf.format(LocalDate.of(Integer.parseInt(year), Integer.parseInt(month), lastDayOfMonth).atTime(23, 59, 59));

        firstHalfFile = NcUtils.createFilename(year, month, version, true);
        secondHalfFile = NcUtils.createFilename(year, month, version, false);

        ncFirst = NcUtils.createNcFile(firstHalfFile, version, timeCoverageStartFirstHalf, timeCoverageEndFirstHalf, 15);
        ncSecond = NcUtils.createNcFile(secondHalfFile, version, timeCoverageStartSecondHalf, timeCoverageEndSecondHalf, lastDayOfMonth - 16);

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
    }

    private static void writeFloatChunk(int x, int y, NetcdfFileWriter ncFile, String varName, float[] data) throws IOException, InvalidRangeException {
        CalvalusLogger.getLogger().info(String.format("Writing data: x=%d, y=%d, 40*40 into variable %s", x, y, varName));

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

    private static int getX(String key) {
        int x = Integer.parseInt(key.substring(12));
        return x * 40;
    }

    private static int getY(String key) {
        int y = Integer.parseInt(key.substring(9, 11));
        return y * 40;
    }

}
