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
import java.time.YearMonth;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Locale;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author thomas
 */
public abstract class AbstractGridReducer extends Reducer<Text, GridCells, NullWritable, NullWritable> {

    protected static final Logger LOG = CalvalusLogger.getLogger();
    private static final int DEFAULT_SCENE_RASTER_HEIGHT = 720;
    private static final int DEFAULT_NUM_LC_CLASSES = 18;

    protected NetcdfFileWriter ncFile;

    protected String outputFilename;
    private GridCells currentGridCells;
    private int targetWidth;
    private int targetHeight;

    protected int numRowsGlobal;
    protected int numLcClasses = DEFAULT_NUM_LC_CLASSES;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        numRowsGlobal = context.getConfiguration().getInt("numRowsGlobal", DEFAULT_SCENE_RASTER_HEIGHT);
        numLcClasses = getNumLcClasses();
        LOG.info("Setup with numLCClasses: " + numLcClasses);
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
        float[] patchNumber = currentGridCells.patchNumber;

        try {
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncFile, "burned_area", burnedAreaFloat);
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncFile, "standard_error", currentGridCells.errors);
            writeFloatChunk(getX(key.toString()), getY(key.toString()), ncFile, "fraction_of_observed_area", currentGridCells.coverage);
            if (addNumPatches()) {
                writeFloatChunk(getX(key.toString()), getY(key.toString()), ncFile, "number_of_patches", patchNumber);
            }

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

    protected boolean addNumPatches() {
        return false;
    }

    protected abstract int getTargetWidth();

    protected abstract int getTargetHeight();

    protected int getNumLcClasses() {
        return numLcClasses;
    };

    protected abstract String getFilename(String year, String month, String version);

    protected abstract NetcdfFileWriter createNcFile(String filename, String version, String timeCoverageStart, String timeCoverageEnd, int numberOfDays) throws IOException;

    private void prepareTargetProducts(Context context) throws IOException {
        String version = context.getConfiguration().get("calvalus.output.version", "v5.1");
        String dateRanges = context.getConfiguration().get("calvalus.input.dateRanges");

        String year;
        String month;
        int lastDayOfMonth;
        if (dateRanges != null) {
            Matcher m = Pattern.compile(".*\\[.*(....-..-..).*:.*(....-..-..).*\\].*").matcher(dateRanges);
            if (! m.matches()) {
                throw new IllegalArgumentException(dateRanges + " is not a date range");
            }
            String timeCoverageStart = m.group(1);
            String timeCoverageEnd = m.group(2);
            year = timeCoverageStart.substring(0,4);
            month = timeCoverageStart.substring(5,7);
            lastDayOfMonth = Integer.parseInt(timeCoverageEnd.substring(8,10));
            LOG.info(String.format("timeCoverageStart: %s", timeCoverageStart));
            LOG.info(String.format("timeCoverageEnd: %s", timeCoverageEnd));
        } else {
            year = context.getConfiguration().get("calvalus.year");
            month = context.getConfiguration().get("calvalus.month");
            if (month.length() < 2) {
                month = "0" + month;
            }
            lastDayOfMonth = YearMonth.of(Integer.parseInt(year), Integer.parseInt(month)).atEndOfMonth().getDayOfMonth();
        }

        String lastDayOfMonthFormatted = String.format((Locale) null, "%02d", lastDayOfMonth);

        outputFilename = getFilename(year, month, version);
        ncFile = createNcFile(outputFilename, version, year + "-" + month + "-" + "01", year + "-" + month + "-" + lastDayOfMonthFormatted, lastDayOfMonth - 1);

        try {
            LOG.info("Writing lat/lon");
            writeLon(ncFile);
            writeLat(ncFile);

            LOG.info("Writing lat/lon bounds");
            writeLonBnds(ncFile);
            writeLatBnds(ncFile);

            LOG.info("Writing time");
            writeTime(ncFile, year, month);
            writeTimeBnds(ncFile, year, month);

            LOG.info("Writing vegetation classes");
            writeVegetationClasses(ncFile);
            writeVegetationClassNames(ncFile);

            LOG.info("Preparing (empty) fraction of burnable area");
            prepareFloatVariable("fraction_of_burnable_area", ncFile);
            prepareFloatVariable("fraction_of_observed_area", ncFile);

            LOG.info("Preparing (empty) burned area and error");
            prepareFloatVariable("burned_area", ncFile);
            prepareFloatVariable("standard_error", ncFile);
            if (addNumPatches()) {
                LOG.info("Preparing (empty) number of patches");
                prepareFloatVariable("number_of_patches", ncFile);
            }

            LOG.info("Preparing (empty) areas");
            prepareAreas("burned_area_in_vegetation_class", numLcClasses, ncFile);
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
        LOG.info("Done preparing target products");
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

    private void writeTimeBnds(NetcdfFileWriter ncFile, String year, String month) throws IOException, InvalidRangeException {
        Variable timeBnds = ncFile.findVariable(getTimeBoundsName());
        double firstDayAsJD = getFirstDayAsJD(year, month);
        int lengthOfMonth = Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).lengthOfMonth();
        float[] array = new float[]{
                (float) (firstDayAsJD),
                (float) (firstDayAsJD + lengthOfMonth - 1)};
        Array values = Array.factory(DataType.FLOAT, new int[]{1, 2}, array);
        ncFile.write(timeBnds, values);
    }

    protected String getTimeBoundsName() {
        return "time_bounds";
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

    protected void writeLonBnds(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable lonBnds = ncFile.findVariable("lon_bounds");
        double[] array = new double[numRowsGlobal * 2 * 2];
        for (int x = 0; x < numRowsGlobal * 2; x++) {
            array[2 * x] = -180.0 + 180.0 * x / numRowsGlobal;
            array[2 * x + 1] = -180.0 + 180.0 * (x + 1) / numRowsGlobal;
        }
        Array values = Array.factory(DataType.DOUBLE, new int[]{numRowsGlobal * 2, 2}, array);
        ncFile.write(lonBnds, values);
    }

    protected void writeLatBnds(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable latBnds = ncFile.findVariable("lat_bounds");
        double[] array = new double[numRowsGlobal * 2];
        for (int y = 0; y < numRowsGlobal; y++) {
            array[2 * y] = 90.0 - 180.0 * y / numRowsGlobal;
            array[2 * y + 1] = 90.0 - 180.0 * (y + 1) / numRowsGlobal;
        }
        Array values = Array.factory(DataType.DOUBLE, new int[]{numRowsGlobal, 2}, array);
        ncFile.write(latBnds, values);
    }

    protected void writeLat(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable lat = ncFile.findVariable("lat");
        double[] array = new double[numRowsGlobal];
        for (int x = 0; x < numRowsGlobal; x++) {
            array[x] = 90.0 - 180.0 / numRowsGlobal / 2 - 180.0 * x / numRowsGlobal;
        }
        Array values = Array.factory(DataType.DOUBLE, new int[]{numRowsGlobal}, array);
        ncFile.write(lat, values);
    }

    protected void writeLon(NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable lon = ncFile.findVariable("lon");
        double[] array = new double[numRowsGlobal * 2];
        for (int x = 0; x < numRowsGlobal * 2; x++) {
            array[x] = -180.0 + 180.0 / numRowsGlobal / 2 + 180.0 * x / numRowsGlobal;
        }
        Array values = Array.factory(DataType.DOUBLE, new int[]{numRowsGlobal * 2}, array);
        ncFile.write(lon, values);
    }

    private void prepareFloatVariable(String varName, NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        Variable variable = ncFile.findVariable(varName);
        float[] array = new float[numRowsGlobal * 2];
        Arrays.fill(array, 0.0F);
        Array values = Array.factory(DataType.FLOAT, new int[]{1, 1, numRowsGlobal * 2}, array);
        for (int y = 0; y < numRowsGlobal; y++) {
            ncFile.write(variable, new int[]{0, y, 0}, values);
        }
    }

    private void prepareAreas(String varName, int lcClassCount, NetcdfFileWriter ncFile) throws IOException, InvalidRangeException {
        LOG.info("preparing areas (" + varName + ")  for " + lcClassCount + " classes ");
        Variable variable = ncFile.findVariable(varName);
        float[] array = new float[numRowsGlobal * 2];
        Arrays.fill(array, 0.0F);
        Array values = Array.factory(DataType.FLOAT, new int[]{1, 1, 1, numRowsGlobal * 2}, array);
        for (int c = 0; c < lcClassCount; ++c) {
            for (int y = 0; y < numRowsGlobal; y++) {
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
     *
     * @param key The mapper key.
     * @return The start y.
     */
    protected int getY(String key) {
        int y = Integer.parseInt(key.substring(9, 11));
        return y * targetHeight;
    }

}
