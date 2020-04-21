package com.bc.calvalus.processing.fire.format.grid;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.ceres.core.Assert;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author thomas
 */
public abstract class AbstractGridReducer extends Reducer<Text, GridCells, NullWritable, NullWritable> {

    private static final int SCENE_RASTER_WIDTH = 1440;
    private static final int SCENE_RASTER_HEIGHT = 720;

    protected NetcdfFileWriter ncFile;

    protected String ncFilename;
    private GridCells currentGridCells;
    private int targetWidth;
    private int targetHeight;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        prepareTargetProducts(context);
        this.targetWidth = getTargetWidth();
        this.targetHeight = getTargetHeight();
    }

    @Override
    protected void reduce(Text key, Iterable<GridCells> values, Context context) throws IOException, InterruptedException {
        Iterator<GridCells> iterator = values.iterator();
        currentGridCells = iterator.next();

        double[] burnedArea = currentGridCells.ba;
        float[] errors = currentGridCells.errors;
        List<double[]> baInLc = currentGridCells.baInLc;
        float[] coverage = currentGridCells.coverage;
        float[] patchNumber = currentGridCells.patchNumber;

        float[] burnedAreaFloat = new float[burnedArea.length];
        for (int i = 0; i < burnedArea.length; i++) {
            burnedAreaFloat[i] = (float) burnedArea[i];
        }

        CalvalusLogger.getLogger().info("Writing for key " + key);

        try {
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncFile, "burned_area", burnedAreaFloat);
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncFile, "standard_error", errors);
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncFile, "fraction_of_observed_area", coverage);
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncFile, "number_of_patches", patchNumber);

            for (int i = 0; i < baInLc.size(); i++) {
                double[] baInClass = baInLc.get(i);
                writeVegetationChunk(key.toString(), i, ncFile, baInClass);
            }
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
    }

    protected abstract void writeVegetationClassNames(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException;

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String outputDir = context.getConfiguration().get("calvalus.output.dir");

        ncFile.close();

        File fileLocation = new File("./" + ncFilename);
        Path path = new Path(outputDir + "/" + ncFilename);
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        if (!fs.exists(path)) {
            FileUtil.copy(fileLocation, fs, path, false, context.getConfiguration());
        }
    }

    protected abstract int getTargetWidth();

    protected abstract int getTargetHeight();

    protected abstract String getFilename(String year, String month, String version);

    protected abstract NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numberOfDays) throws IOException;

    private void prepareTargetProducts(Context context) throws IOException {
        String year = context.getConfiguration().get("calvalus.year");
        String month = context.getConfiguration().get("calvalus.month");
        String version = context.getConfiguration().get("calvalus.version", "v5.1");
        Assert.notNull(year, "calvalus.year");
        Assert.notNull(month, "calvalus.month");

        int lastDayOfMonth = Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).lengthOfMonth();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneId.systemDefault());
        String timeCoverageStart = dtf.format(LocalDate.of(Integer.parseInt(year), Integer.parseInt(month), 1).atTime(0, 0, 0));
        String timeCoverageEnd = dtf.format(LocalDate.of(Integer.parseInt(year), Integer.parseInt(month), lastDayOfMonth).atTime(23, 59, 59));

        ncFilename = getFilename(year, month, version);

        ncFile = createNcFile(ncFilename, version, timeCoverageStart, timeCoverageEnd, lastDayOfMonth);

        try {
            writeLon(ncFile);
            writeLat(ncFile);

            writeLonBnds(ncFile);
            writeLatBnds(ncFile);

            writeTime(ncFile, year, month);
            writeTimeBnds(ncFile, year, month);

            writeVegetationClasses(ncFile);
            writeVegetationClassNames(ncFile);

            prepareFraction("fraction_of_burnable_area", ncFile);
            prepareFraction("fraction_of_observed_area", ncFile);
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
    }

    protected GridCells getCurrentGridCells() {
        return currentGridCells;
    }

    protected void writeFloatChunk(int x, int y, NetcdfFileWriter ncFile, String varName, float[] data) throws IOException, InvalidRangeException {
        CalvalusLogger.getLogger().info(String.format("Writing data: x=%d, y=%d, %d*%d into variable %s", x, y, targetWidth, targetHeight, varName));

        Variable variable = ncFile.findVariable(varName);
        Array values = Array.factory(DataType.FLOAT, new int[]{1, targetWidth, targetHeight}, data);
        ncFile.write(variable, new int[]{0, y, x}, values);
    }

    private void writeVegetationChunk(String key, int lcClassIndex, NetcdfFileWriter ncFile, double[] baInClass) throws IOException, InvalidRangeException {
        int x = getX(key);
        int y = getY(key);
        CalvalusLogger.getLogger().info(String.format("Writing data: x=%d, y=%d, %d*%d into lc class %d", x, y, targetWidth, targetHeight, lcClassIndex));

        Variable variable = ncFile.findVariable("burned_area_in_vegetation_class");
        float[] f = new float[baInClass.length];
        for (int i = 0; i < baInClass.length; i++) {
            f[i] = (int) baInClass[i];
        }

        Array values = Array.factory(DataType.FLOAT, new int[]{1, 1, targetWidth, targetHeight}, f);
        ncFile.write(variable, new int[]{0, lcClassIndex, y, x}, values);
    }

    protected abstract void writeVegetationClasses(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException;

    private static void writeTimeBnds(NetcdfFileWriter ncFile, String year, String month) throws IOException, InvalidRangeException {
        Variable timeBnds = ncFile.findVariable("time_bnds");
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

    private static void prepareFraction(String varName, NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable variable = ncFile.findVariable(varName);
        float[] array = new float[SCENE_RASTER_WIDTH];
        Arrays.fill(array, 0.0F);
        Array values = Array.factory(DataType.FLOAT, new int[]{1, 1, SCENE_RASTER_WIDTH}, array);
        for (int y = 0; y < 720; y++) {
            ncFile.write(variable, new int[]{0, y, 0}, values);
        }
    }

    /**
     * Returns the start x for the given tile, where <code>targetWidth * targetHeight</code> many values are written to
     * the target raster.
     *
     * @param key The mapper key.
     * @return The start x.
     */
    protected int getX(String key) {
        int x = Integer.parseInt(key.substring(12));
        return x * targetWidth;
    }

    /**
     * Returns the start y for the given tile, where <code>targetWidth * targetHeight</code> many values are written to
     * the target raster.
     * @param key The mapper key.
     * @return The start y.
     */
    protected int getY(String key) {
        int y = Integer.parseInt(key.substring(9, 11));
        return y * targetHeight;
    }

}
