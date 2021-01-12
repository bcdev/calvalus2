/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.l3.HadoopBinManager;
import com.bc.calvalus.processing.l3.L3SpatialBin;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.snap.binning.*;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.support.ObservationImpl;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.util.StringUtils;
import org.esa.snap.dataio.netcdf.util.NetcdfFileOpener;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads an FRP product and produces (binIndex, spatialBin) pairs.
 *
 * @author boe, tb
 */
public class FrpMapper extends Mapper<NullWritable, NullWritable, LongWritable, L3SpatialBin> {

    static final int CONF_IN_UNFILLED = 32;
    static final int FRP_CLOUD = 32;
    static final int L1B_WATER = 2;
    static final int FRP_WATER = 4;
    static final int DAY = 64;
    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final SimpleDateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static final SimpleDateFormat COMPACT_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd'T'HHmmss");
    private static final int S3A_OFFSET = 0;   // offset into variables array for s3a data
    private static final int S3B_OFFSET = 6;   // offset into variables array for s3b data

    static String[] VARIABLE_NAMES = {
            "s3a_night_pixel",
            "s3a_night_cloud",
            "s3a_night_water",
            "s3a_night_fire",
            "s3a_night_frp",
            "s3a_night_frp_unc",
            "s3b_night_pixel",
            "s3b_night_cloud",
            "s3b_night_water",
            "s3b_night_fire",
            "s3b_night_frp",
            "s3b_night_frp_unc"
    };

    static String[] VARIABLE_NAMES_MONTHLY = {
            "s3a_fire_land_pixel",
            "s3a_fire_water_pixel",
            "s3a_frp_mir_land",
            "s3a_frp_mir_land_unc",
            "s3a_cloud_land_pixel",
            "s3a_water_pixel",
            "s3a_slstr_pixel",
            "s3b_fire_land_pixel",
            "s3b_fire_water_pixel",
            "s3b_frp_mir_land",
            "s3b_frp_mir_land_unc",
            "s3b_cloud_land_pixel",
            "s3b_water_pixel",
            "s3b_slstr_pixel",
    };
    private static long YEAR2k_MILLIS = Long.MIN_VALUE;

    static {
        ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        COMPACT_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));

        try {
            YEAR2k_MILLIS = ISO_DATE_FORMAT.parse("2000-01-01T00:00:00.000Z").getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    static HashMap<Integer, Integer> createFiresLUT(Array[] frpArrays, int numFires, int columns) {
        final HashMap<Integer, Integer> fireIndex = new HashMap<>();
        final int rowIndex = FRP_VARIABLES.j.ordinal();
        final int colIndex = FRP_VARIABLES.i.ordinal();
        for (int i = 0; i < numFires; ++i) {
            final int row = frpArrays[rowIndex].getInt(i);
            final int col = frpArrays[colIndex].getShort(i);
            fireIndex.put(row * columns + col, i);
        }
        return fireIndex;
    }

    private static double extractMJDFromFilename(Path inputPath) {
        final double mjd;
        try {
            final String dateString = copyDateString(inputPath);
            final ProductData.UTC utc = ProductData.UTC.parse(dateString, COMPACT_DATE_FORMAT);
            mjd = utc.getMJD();
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return mjd;
    }

    static String copyDateString(Path inputPath) {
        return inputPath.getName().substring(16, 31);
    }

    static int getPlatformNumber(Path inputPath) {
        final String fileName = inputPath.getName();
        if (fileName.startsWith("S3A")) {
            return 1;
        } else if (fileName.startsWith("S3B")) {
            return 2;
        }

        throw new IllegalArgumentException("Unknown Sentinel platform");
    }

    static int getSensorOffset(int platformNumber) {
        if (platformNumber == 1) {
            return S3A_OFFSET;
        } else if (platformNumber == 2) {
            return S3B_OFFSET;
        }

        throw new IllegalArgumentException("Unknown Sentinel platform");
    }

    static int[] createVariableIndex(BinningContext binningContext, String[] variableNames) {
        final VariableContext variableContext = binningContext.getVariableContext();
        final int variableCount = variableContext.getVariableCount();
        if (variableCount != variableNames.length) {
            throw new IllegalArgumentException("Number of configured variables does not match required.");
        }

        final int[] indices = new int[variableNames.length];
        for (int i = 0; i < variableCount; i++) {
            indices[i] = variableContext.getVariableIndex(variableNames[i]);
            if (indices[i] < 0) {
                throw new IllegalArgumentException("Variable missing in configuration: " + variableNames[i]);
            }
        }

        return indices;
    }

    /**
     * parses the time range string supplied to Calvalus and returns min and max date in microseconds since 2000-01-01
     *
     * @param rangeString the time range string in Calvalus syntax
     * @return array of min/max times in micros since 2000-01-01
     */
    static long[] getTimeRange(String rangeString) throws IOException {
        // remove square brackets
        final String substring = rangeString.substring(1, rangeString.length() - 1);
        final String[] tokens = StringUtils.split(substring, new char[]{':'}, true);

        try {
            final ProductData.UTC minDate = ProductData.UTC.parse(tokens[0], "yyyy-MM-dd");
            final ProductData.UTC maxDate = ProductData.UTC.parse(tokens[1], "yyyy-MM-dd");
            final Calendar minCalendar = minDate.getAsCalendar();
            final Calendar maxCalendar = maxDate.getAsCalendar();
            maxCalendar.set(Calendar.HOUR_OF_DAY, 23);
            maxCalendar.set(Calendar.MINUTE, 59);
            maxCalendar.set(Calendar.SECOND, 59);
            maxCalendar.set(Calendar.MILLISECOND, 999);

            final long minMicros = (minCalendar.getTimeInMillis() - YEAR2k_MILLIS) * 1000L;
            final long maxMicros = (maxCalendar.getTimeInMillis() - YEAR2k_MILLIS) * 1000L;

            return new long[]{minMicros, maxMicros};
        } catch (ParseException e) {
            throw new IOException("Unable to parse date-range: " + e.getMessage());
        }
    }

    static int isCloud(int flags) {
        return (flags & FRP_CLOUD) != 0 ? 1 : 0;
    }

    static boolean isUnfilled(int confFlags) {
        return (confFlags & CONF_IN_UNFILLED) != 0;
    }

    // package access for testing only tb 2020-12-16
    static boolean isWater(int flags) {
        return (flags & (L1B_WATER | FRP_WATER)) != 0;
    }

    // package access for testing only tb 2020-12-17
    static boolean isDay(int flags) {
        return (flags & (DAY)) != 0;
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        final String targetFormat = conf.get("calvalus.targetFormat", "l2monthly");  // one of l2monthly, l3daily, l3cycle, l3monthly

        final Path inputPath = ((FileSplit) context.getInputSplit()).getPath();
        final File[] inputFiles = CalvalusProductIO.uncompressArchiveToCWD(inputPath, conf);
        final int platformNumber = getPlatformNumber(inputPath);

        final Array[] frpArrays = new Array[FRP_VARIABLES.values().length];
        int numFires = readFrpVariables(inputFiles, frpArrays);
        final Array confidenceFlags = readConfidenceFlags(inputFiles);

        if ("l2monthly".equals(targetFormat)) {
            final int count = writeL2MonthlyBin(context, platformNumber, frpArrays, confidenceFlags, numFires);
            LOG.info(count + "/" + numFires + " records streamed of " + inputPath.getName());
            return;
        }

        final boolean onlyLand = conf.getBoolean("calvalus.onlyLand", true);
        final boolean onlyNight = conf.getBoolean("calvalus.onlyNight", true);

        final Array[] geodeticArrays = new Array[GEODETIC_VARIABLES.values().length];
        final int[] rowCol = readGeodeticVariables(inputFiles, geodeticArrays);
        final int latIndex = GEODETIC_VARIABLES.latitude_in.ordinal();
        final int lonIndex = GEODETIC_VARIABLES.longitude_in.ordinal();

        final Geometry regionGeometry = GeometryUtils.createGeometry(conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        final BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
        final DataPeriod dataPeriod = HadoopBinManager.createDataPeriod(conf, binningConfig.getMinDataHour());
        final BinningContext binningContext = HadoopBinManager.createBinningContext(binningConfig, dataPeriod, regionGeometry);
        final SpatialBinEmitter spatialBinEmitter = new SpatialBinEmitter(context);
        final SpatialBinner spatialBinner = new SpatialBinner(binningContext, spatialBinEmitter);
        final Index index = frpArrays[FRP_VARIABLES.flags.ordinal()].getIndex();

        // create lut of fires by row and column
        final HashMap<Integer, Integer> fireIndex = createFiresLUT(frpArrays, numFires, rowCol[1]);

        final double mjd = extractMJDFromFilename(inputPath);
        final int areaIndex = FRP_VARIABLES.IFOV_area.ordinal();
        final int mwirIndex = FRP_VARIABLES.FRP_MWIR.ordinal();
        final int mwirUncIndex = FRP_VARIABLES.FRP_uncertainty_MWIR.ordinal();

/*
        if ("l3daily".equals(targetFormat) || "l3cycle".equals(targetFormat)) {
*/
            // pixel loop
            int count = 0;
            int variableOffset = getSensorOffset(platformNumber);
            // create observation variable sequence
            final int[] variableIndex = createVariableIndex(binningContext, VARIABLE_NAMES);

            final int rows = rowCol[0];
            final int columns = rowCol[1];
            for (int row = 0; row < rows; ++row) {
                final List<Observation> observations = new ArrayList<>();
                for (int col = 0; col < columns; ++col) {
                    index.set(row, col);

                    // apply flags
                    final int flags = frpArrays[FRP_VARIABLES.flags.ordinal()].getInt(index);
//                    if (isWater(flags) && onlyLand) {
//                        continue;
//                    }
                    if (isDay(flags) && onlyNight) {
                        continue;
                    }
                    final int confFlags = confidenceFlags.getInt(index);
                    if (isUnfilled(confFlags)) {
                        continue;
                    }

                    // construct observation
                    final double lat = geodeticArrays[latIndex].getInt(index) * 1e-6;
                    final double lon = geodeticArrays[lonIndex].getInt(index) * 1e-6;

                    float frpMwir = Float.NaN;
                    float frpMwirUnc = Float.NaN;
                    if (! (isWater(flags) && onlyLand)) {
                        final Integer fire = fireIndex.get(row * columns + col);
                        if (fire != null) {
                            final double area = frpArrays[areaIndex].getDouble(fire);
                            if (area > 0.0) {
                                frpMwir = (float) frpArrays[mwirIndex].getDouble(fire);
                                if (frpMwir <= 0.0) {
                                    frpMwir = Float.NaN;
                                } else {
                                    ++count;
                                }
                                frpMwirUnc = (float) frpArrays[mwirUncIndex].getDouble(fire);
                            }
                        }
                    }

                    int emptyOffset = 6 - variableOffset;
                    // aggregate contributions based on flags and platform
                    float[] values = new float[12];
                    values[variableIndex[variableOffset + 0]] = 1; // total measurement count
                    values[variableIndex[variableOffset + 1]] = isWater(flags) ? 0 : isCloud(flags);  // frp_cloud
                    values[variableIndex[variableOffset + 2]] = isWater(flags) ? 1 : 0;  // l1b_water | frp_water
                    values[variableIndex[variableOffset + 3]] = Float.isNaN(frpMwir) ? 0 : 1;  // valid fire count
                    values[variableIndex[variableOffset + 4]] = frpMwir;
                    values[variableIndex[variableOffset + 5]] = frpMwirUnc * frpMwirUnc;   // squared uncertainty
                    values[variableIndex[emptyOffset + 0]] = Float.NaN;
                    values[variableIndex[emptyOffset + 1]] = Float.NaN;
                    values[variableIndex[emptyOffset + 2]] = Float.NaN;
                    values[variableIndex[emptyOffset + 3]] = Float.NaN;
                    values[variableIndex[emptyOffset + 4]] = Float.NaN;
                    values[variableIndex[emptyOffset + 5]] = Float.NaN;
                    observations.add(new ObservationImpl(lat, lon, mjd, values));
                }
                spatialBinner.processObservationSlice(observations);
            }
            spatialBinner.complete();
            LOG.info(count + "/" + numFires + " records and " +
                    spatialBinEmitter.numObsTotal + "/" + spatialBinEmitter.numBinsTotal +
                    " obs/bins streamed of " + inputPath.getName());
            final Exception[] exceptions = spatialBinner.getExceptions();
            for (Exception exception : exceptions) {
                String m = MessageFormat.format("Failed to process input slice of {0}", inputPath.getName());
                LOG.log(Level.SEVERE, m, exception);
            }
/*
        } else if ("l3monthly".equals(targetFormat)) {
            // create observation variable sequence
            final int[] variableIndex = createVariableIndex(binningContext, VARIABLE_NAMES_MONTHLY);

            final int rows = rowCol[0];
            final int columns = rowCol[1];
            for (int row = 0; row < rows; ++row) {
                final List<Observation> observations = new ArrayList<>();
                for (int col = 0; col < columns; ++col) {
                    index.set(row, col);

                    // apply flags
                    final int flags = frpArrays[FRP_VARIABLES.flags.ordinal()].getInt(index);
                    if (isWater(flags) && onlyLand) {
                        continue;
                    }
                    if (isDay(flags) && onlyNight) {
                        continue;
                    }
                    final int confFlags = confidenceFlags.getInt(index);
                    if (isUnfilled(confFlags)) {
                        continue;
                    }

                    // construct observation
                    final double lat = geodeticArrays[latIndex].getInt(index) * 1e-6;
                    final double lon = geodeticArrays[lonIndex].getInt(index) * 1e-6;

                    final Integer fire = fireIndex.get(row * columns + col);
                    float frpMwir = Float.NaN;
                    float frpMwirUnc = Float.NaN;
                    if (fire != null) {
                        final double area = frpArrays[areaIndex].getDouble(fire);
                        if (area > 0.0) {
                            frpMwir = (float) frpArrays[mwirIndex].getDouble(fire);
                            if (frpMwir <= 0.0) {
                                frpMwir = Float.NaN;
                            } else {
                                frpMwirUnc = (float) frpArrays[mwirUncIndex].getDouble(fire);
                            }
                        }
                    }

                    float[] values = new float[VARIABLE_NAMES_MONTHLY.length];
                    final boolean isWater = isWater(flags);
                    final boolean isFire = !Float.isNaN(frpMwir);
                    final boolean isCloud = isCloud(flags) > 0;
                    float Nlf = 0.f;
                    if (isFire && !isWater) {
                        Nlf = 1.f;
                    }

                    float Nwf = 0.f;
                    if (isFire && isWater) {
                        Nwf = 1.f;
                    }

                    float Frp = Float.NaN;
                    float FrpUncSquared = Float.NaN;
                    if (isFire && !isWater) {
                        Frp = frpMwir;
                        FrpUncSquared = frpMwirUnc * frpMwirUnc;
                    }

                    float Ncl = 0.f;
                    if (isCloud && !isWater) {
                        Ncl = 1.f;
                    }

                    if (platformNumber == 1) {
                        values[variableIndex[0]] = Nlf;
                        values[variableIndex[1]] = Nwf;
                        values[variableIndex[2]] = Frp;
                        values[variableIndex[3]] = FrpUncSquared;
                        values[variableIndex[4]] = Ncl;
                        values[variableIndex[5]] = isWater ? 1 : 0;    // Nw
                        values[variableIndex[6]] = 1; // No
                        values[variableIndex[S3B_P1M_OFFSET + 0]] = Float.NaN;
                        values[variableIndex[S3B_P1M_OFFSET + 1]] = Float.NaN;
                        values[variableIndex[S3B_P1M_OFFSET + 2]] = Float.NaN;
                        values[variableIndex[S3B_P1M_OFFSET + 3]] = Float.NaN;
                        values[variableIndex[S3B_P1M_OFFSET + 4]] = Float.NaN;
                        values[variableIndex[S3B_P1M_OFFSET + 5]] = Float.NaN;
                        values[variableIndex[S3B_P1M_OFFSET + 6]] = Float.NaN;
                    } else {
                        values[variableIndex[0]] = Float.NaN;
                        values[variableIndex[1]] = Float.NaN;
                        values[variableIndex[2]] = Float.NaN;
                        values[variableIndex[3]] = Float.NaN;
                        values[variableIndex[4]] = Float.NaN;
                        values[variableIndex[5]] = Float.NaN;
                        values[variableIndex[6]] = Float.NaN;
                        values[variableIndex[S3B_P1M_OFFSET + 0]] = Nlf;
                        values[variableIndex[S3B_P1M_OFFSET + 1]] = Nwf;
                        values[variableIndex[S3B_P1M_OFFSET + 2]] = Frp;
                        values[variableIndex[S3B_P1M_OFFSET + 3]] = FrpUncSquared;
                        values[variableIndex[S3B_P1M_OFFSET + 4]] = Ncl;
                        values[variableIndex[S3B_P1M_OFFSET + 5]] = isWater ? 1 : 0;    // Nw
                        values[variableIndex[S3B_P1M_OFFSET + 6]] = 1; // No
                    }
                    observations.add(new ObservationImpl(lat, lon, mjd, values));
                }
                spatialBinner.processObservationSlice(observations);
            }
            spatialBinner.complete();
        }
*/
    }

    private int[] readGeodeticVariables(File[] inputFiles, Array[] geodeticArrays) throws IOException {
        final File geodeticFile = findByName("geodetic_in.nc", inputFiles);
        final int[] rowCol;
        try (NetcdfFile geodeticNetcdf = NetcdfFileOpener.open(geodeticFile.getPath())) {
            for (GEODETIC_VARIABLES v : GEODETIC_VARIABLES.values()) {
                geodeticArrays[v.ordinal()] = geodeticNetcdf.findVariable(v.name()).read();
            }

            final int columns = geodeticNetcdf.findDimension("columns").getLength();
            final int rows = geodeticNetcdf.findDimension("rows").getLength();

            rowCol = new int[]{rows, columns};
        }

        return rowCol;
    }

    private int readFrpVariables(File[] inputFiles, Array[] frpArrays) throws IOException {
        final File frpFile = findByName("FRP_in.nc", inputFiles);

        int numFires = -1;
        try (NetcdfFile frpNetcdf = NetcdfFileOpener.open(frpFile.getPath())) {
            for (FRP_VARIABLES v : FRP_VARIABLES.values()) {
                frpArrays[v.ordinal()] = frpNetcdf.findVariable(v.name()).read();
            }
            numFires = frpNetcdf.findDimension("fires").getLength();
        }

        return numFires;
    }

    private Array readConfidenceFlags(File[] inputFiles) throws IOException {
        final File flagsFile = findByName("flags_in.nc", inputFiles);
        try (NetcdfFile flagsNetcdf = NetcdfFileOpener.open(flagsFile.getPath())) {
            final Variable confidence_in = flagsNetcdf.findVariable("confidence_in");
            return confidence_in.read();
        }
    }

    private int writeL2MonthlyBin(Context context, float platformNumber, Array[] frpArrays, Array confidenceFlags, int numFires) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        final String dateRanges = conf.get("calvalus.input.dateRanges", null);
        final long[] timeRange = getTimeRange(dateRanges);
        final boolean onlyLand = conf.getBoolean("calvalus.onlyLand", false);
        final boolean onlyNight = conf.getBoolean("calvalus.onlyNight", false);

        int count = 0;
        final Index flagsIdx = frpArrays[FRP_VARIABLES.flags.ordinal()].getIndex();
        final Index confIdx = confidenceFlags.getIndex();
        for (int i = 0; i < numFires; ++i) {
            // filter time
            final long time = frpArrays[FRP_VARIABLES.time.ordinal()].getLong(i);
            if (time < timeRange[0] || time > timeRange[1]) {
                continue;
            }

            // filter flags
            final int row = frpArrays[FRP_VARIABLES.j.ordinal()].getInt(i);
            final int col = frpArrays[FRP_VARIABLES.i.ordinal()].getShort(i);
            final int flags = frpArrays[FRP_VARIABLES.flags.ordinal()].getInt(flagsIdx.set(row, col));
            if (isWater(flags) && onlyLand) {
                continue;
            }
            if (isDay(flags) && onlyNight) {
                continue;
            }
            confIdx.set(row, col);
            final int confFlags = confidenceFlags.getInt(confIdx);
            if (isUnfilled(confFlags)) {
                continue;
            }

            final double frpMwir = frpArrays[FRP_VARIABLES.FRP_MWIR.ordinal()].getDouble(i);
            final double area = frpArrays[FRP_VARIABLES.IFOV_area.ordinal()].getDouble(i);
            if (area <= 0.0) {
                LOG.info("skipping empty area record at time " + time);
                continue;
            }
            if (frpMwir <= 0.0) {
                LOG.info("skipping non-MWIR record at time " + time);
                continue;
            }

            final double latitude = frpArrays[FRP_VARIABLES.latitude.ordinal()].getDouble(i);
            final double longitude = frpArrays[FRP_VARIABLES.longitude.ordinal()].getDouble(i);
            final double frpSwir = frpArrays[FRP_VARIABLES.FRP_SWIR.ordinal()].getDouble(i);
            final double frpMwirUnc = frpArrays[FRP_VARIABLES.FRP_uncertainty_MWIR.ordinal()].getDouble(i);
            final double frpSwirUnc = frpArrays[FRP_VARIABLES.FRP_uncertainty_SWIR.ordinal()].getDouble(i);
            final int used_channel = frpArrays[FRP_VARIABLES.used_channel.ordinal()].getInt(i);
            final int classification = frpArrays[FRP_VARIABLES.classification.ordinal()].getInt(i);
            final double confidence = frpArrays[FRP_VARIABLES.confidence.ordinal()].getDouble(i);

            ++count;
            //
            // create and write one bin with a record of FRP values
            final L3SpatialBin bin = new L3SpatialBin(time, FRP_VARIABLES.values().length, 0);
            final float[] featureValues = bin.getFeatureValues();

            featureValues[0] = platformNumber;
            featureValues[FRP_VARIABLES.latitude.ordinal()] = (float) latitude;
            featureValues[FRP_VARIABLES.longitude.ordinal()] = (float) longitude;
            featureValues[FRP_VARIABLES.j.ordinal()] = (float) row;
            featureValues[FRP_VARIABLES.i.ordinal()] = (float) col;
            featureValues[FRP_VARIABLES.FRP_MWIR.ordinal()] = (float) frpMwir;
            featureValues[FRP_VARIABLES.FRP_uncertainty_MWIR.ordinal()] = (float) frpMwirUnc;
            featureValues[FRP_VARIABLES.FRP_SWIR.ordinal()] = (float) frpSwirUnc;
            featureValues[FRP_VARIABLES.FRP_uncertainty_SWIR.ordinal()] = (float) frpSwir;
            featureValues[FRP_VARIABLES.IFOV_area.ordinal()] = (float) area;
            featureValues[FRP_VARIABLES.flags.ordinal()] = (float) flags;
            featureValues[FRP_VARIABLES.used_channel.ordinal()] = (float) used_channel;
            featureValues[FRP_VARIABLES.classification.ordinal()] = (float) classification;
            featureValues[FRP_VARIABLES.confidence.ordinal()] = (float) confidence;
            context.write(new LongWritable(time), bin);
        }

        return count;
    }

    private File findByName(String name, File[] files) {
        for (File f : files) {
            if (f.getName().equals(name)) {
                return f;
            }
        }
        throw new NoSuchElementException(name + " not found");
    }

    enum FRP_VARIABLES {
        time,
        latitude,
        longitude,
        j,
        i,
        FRP_MWIR,
        FRP_uncertainty_MWIR,
        FRP_SWIR,
        FRP_uncertainty_SWIR,
        IFOV_area,
        flags,
        used_channel,
        classification,
        confidence
    }

    enum GEODETIC_VARIABLES {
        latitude_in,
        longitude_in
    }

    private static class SpatialBinEmitter implements SpatialBinConsumer {
        int numObsTotal = 0;
        int numBinsTotal = 0;
        private Context context;

        SpatialBinEmitter(Context context) {
            this.context = context;
        }

        @Override
        public void consumeSpatialBins(BinningContext binningContext, List<SpatialBin> spatialBins) throws Exception {
            for (SpatialBin spatialBin : spatialBins) {
                context.write(new LongWritable(spatialBin.getIndex()), (L3SpatialBin) spatialBin);
                numObsTotal += spatialBin.getNumObs();
                numBinsTotal++;
            }
        }
    }
}
