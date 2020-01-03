package com.bc.calvalus.processing.fire.format.grid;

import com.bc.calvalus.commons.CalvalusLogger;
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
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author thomas
 */
public abstract class AbstractGridReducer extends Reducer<Text, GridCells, NullWritable, NullWritable> {

    protected static final Logger LOG = CalvalusLogger.getLogger();

    private static final int SCENE_RASTER_WIDTH = 1440;
    private static final int SCENE_RASTER_HEIGHT = 720;

    protected NetcdfFileWriter ncFile;

    protected String outputFilename;
    private GridCells currentGridCells;
    private int targetWidth;
    private int targetHeight;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        prepareTargetProducts(context);
        this.targetWidth = getTargetWidth();
        this.targetHeight = getTargetHeight();
        LOG.info(outputFilename + " prepared, height=" + targetHeight + ", width=" + targetWidth);
    }

    @Override
    protected void reduce(Text key, Iterable<GridCells> values, Context context) throws IOException, InterruptedException {
        currentGridCells = values.iterator().next();

        float[] burnedAreaFloat = new float[currentGridCells.ba.length];
        for (int i = 0; i < currentGridCells.ba.length; i++) {
            burnedAreaFloat[i] = (float) currentGridCells.ba[i];
        }

        try {
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncFile, "burned_area", burnedAreaFloat);
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncFile, "standard_error", currentGridCells.errors);
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncFile, "fraction_of_observed_area", currentGridCells.coverage);

            for (int i = 0; i < currentGridCells.baInLc.size(); i++) {
                double[] baInClass = currentGridCells.baInLc.get(i);
                writeVegetationChunk(key.toString(), i, ncFile, baInClass);
            }
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }

        LOG.info(String.format("chunk %s written to output %s", key, outputFilename));

    }

    protected abstract void writeVegetationClassNames(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException;

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String outputDir = context.getConfiguration().get("calvalus.output.dir");

        ncFile.close();

        File fileLocation = new File("./" + outputFilename);
        Path path = new Path(outputDir + "/" + outputFilename);
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        if (!fs.exists(path)) {
            FileUtil.copy(fileLocation, fs, path, false, context.getConfiguration());
            LOG.info(String.format("output file %s archived in %s", outputFilename, outputDir));
        } else {
            LOG.warning(String.format("output file %s not archived in %s, file exists", outputFilename, outputDir));
        }
    }

    protected abstract int getTargetWidth();

    protected abstract int getTargetHeight();

    protected abstract String getFilename(String year, String month, String version);

    protected abstract NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numberOfDays) throws IOException;

    private void prepareTargetProducts(Context context) throws IOException {
        String dateRanges = context.getConfiguration().get("calvalus.input.dateRanges");
        String version = context.getConfiguration().get("calvalus.output.version", "v5.1");
        // set derived parameters
        Matcher m = Pattern.compile(".*\\[.*(....-..-..).*:.*(....-..-..).*\\].*").matcher(dateRanges);
        if (! m.matches()) {
            throw new IllegalArgumentException(dateRanges + " is not a date range");
        }
        String timeCoverageStart = m.group(1);
        String timeCoverageEnd = m.group(2);
        String year = timeCoverageStart.substring(0,4);
        String month = timeCoverageStart.substring(5,7);
        int lastDayOfMonth = Integer.parseInt(timeCoverageEnd.substring(8,10));

        outputFilename = getFilename(year, month, version);

        ncFile = createNcFile(outputFilename, version, timeCoverageStart, timeCoverageEnd, lastDayOfMonth);

        try {
            writeLon(ncFile);
            writeLat(ncFile);

            writeLonBnds(ncFile);
            writeLatBnds(ncFile);

            writeTime(ncFile, year, month);
            writeTimeBnds(ncFile, year, month);

            writeVegetationClasses(ncFile);
            writeVegetationClassNames(ncFile);

            prepareFloatVariable("fraction_of_burnable_area", ncFile);
            prepareFloatVariable("fraction_of_observed_area", ncFile);

            prepareFloatVariable("burned_area", ncFile);
            prepareFloatVariable("standard_error", ncFile);

            prepareAreas("burned_area_in_vegetation_class", 18, ncFile);
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
    }

    protected GridCells getCurrentGridCells() {
        return currentGridCells;
    }

    protected void writeFloatChunk(int x, int y, NetcdfFileWriter ncFile, String varName, float[] data) throws IOException, InvalidRangeException {
        LOG.fine(String.format("Writing data: x=%d, y=%d, %d*%d into variable %s", x, y, targetWidth, targetHeight, varName));

        Variable variable = ncFile.findVariable(varName);
        Array values = Array.factory(DataType.FLOAT, new int[]{1, targetWidth, targetHeight}, data);
        ncFile.write(variable, new int[]{0, y, x}, values);
    }

    private void writeVegetationChunk(String key, int lcClassIndex, NetcdfFileWriter ncFile, double[] baInClass) throws IOException, InvalidRangeException {
        int x = getX(key);
        int y = getY(key);
        LOG.fine(String.format("Writing data: x=%d, y=%d, %d*%d into lc class %d", x, y, targetWidth, targetHeight, lcClassIndex));

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

    private static void prepareFloatVariable(String varName, NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable variable = ncFile.findVariable(varName);
        float[] array = new float[SCENE_RASTER_WIDTH];
        Arrays.fill(array, 0.0F);
        Array values = Array.factory(DataType.FLOAT, new int[]{1, 1, SCENE_RASTER_WIDTH}, array);
        for (int y = 0; y < 720; y++) {
            ncFile.write(variable, new int[]{0, y, 0}, values);
        }
    }

    private static void prepareAreas(String varName, int lcClassCount, NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable variable = ncFile.findVariable(varName);
        float[] array = new float[SCENE_RASTER_WIDTH];
        Arrays.fill(array, 0.0F);
        Array values = Array.factory(DataType.FLOAT, new int[]{1, 1, 1, SCENE_RASTER_WIDTH}, array);
        for (int c=0; c<lcClassCount; ++c) {
            for (int y = 0; y < 720; y++) {
                ncFile.write(variable, new int[]{0, c, y, 0}, values);
            }
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
