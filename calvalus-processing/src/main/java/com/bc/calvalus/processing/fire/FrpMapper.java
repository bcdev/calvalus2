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
import org.esa.snap.dataio.netcdf.util.NetcdfFileOpener;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.NetcdfFile;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads an FRP product and produces (binIndex, spatialBin) pairs.
 *
 * @author boe
 */
public class FrpMapper extends Mapper<NullWritable, NullWritable, LongWritable, L3SpatialBin> {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final SimpleDateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static final SimpleDateFormat COMPACT_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd'T'HHmmss");

    private static final int S3B_OFFSET = 10;   // offset into variables array for s3b data

    static {
        ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        COMPACT_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    static HashMap<Integer, Integer> createFiresLUT(Array[] frpArrays, int numFires, int columns) {
        final HashMap<Integer, Integer> fireIndex = new HashMap<>();
        final int rowIndex = FRP_VARIABLES.j.ordinal();
        final int colIndex = FRP_VARIABLES.i.ordinal();
        for (int i = 0; i < numFires; ++i) {
            int row = frpArrays[rowIndex].getInt(i);
            int col = frpArrays[colIndex].getShort(i);
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

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        final String targetFormat = conf.get("calvalus.targetFormat", "l2monthly");  // one of l2monthly, l3daily, l3cycle, l3monthly

        final Path inputPath = ((FileSplit) context.getInputSplit()).getPath();
        final File[] inputFiles = CalvalusProductIO.uncompressArchiveToCWD(inputPath, conf);
        final int platformNumber = inputPath.getName().startsWith("S3A") ? 1 : inputPath.getName().startsWith("S3B") ? 2 : 0;

        final File frpFile = findByName("FRP_in.nc", inputFiles);
        final NetcdfFile frpNetcdf = openNetcdfFile(frpFile);
        final Array[] frpArrays = new Array[FRP_VARIABLES.values().length];
        for (FRP_VARIABLES v : FRP_VARIABLES.values()) {
            frpArrays[v.ordinal()] = frpNetcdf.findVariable(v.name()).read();
        }

        int numFires = frpNetcdf.findDimension("fires").getLength();

        if ("l2monthly".equals(targetFormat)) {
            final int count = writeL2MonthlyBin(context, platformNumber, frpArrays, numFires);
            LOG.info(count + "/" + numFires + " records streamed of " + inputPath.getName());
            return;
        }

        final File geodeticFile = findByName("geodetic_in.nc", inputFiles);
        final NetcdfFile geodeticNetcdf = openNetcdfFile(geodeticFile);
        final Array[] geodeticArrays = new Array[GEODETIC_VARIABLES.values().length];
        for (GEODETIC_VARIABLES v : GEODETIC_VARIABLES.values()) {
            geodeticArrays[v.ordinal()] = geodeticNetcdf.findVariable(v.name()).read();
        }
        final int columns = geodeticNetcdf.findDimension("columns").getLength();
        final int rows = geodeticNetcdf.findDimension("rows").getLength();

        final Geometry regionGeometry = GeometryUtils.createGeometry(conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        final BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
        final DataPeriod dataPeriod = HadoopBinManager.createDataPeriod(conf, binningConfig.getMinDataHour());
        final BinningContext binningContext = HadoopBinManager.createBinningContext(binningConfig, dataPeriod, regionGeometry);
        final SpatialBinEmitter spatialBinEmitter = new SpatialBinEmitter(context);
        final SpatialBinner spatialBinner = new SpatialBinner(binningContext, spatialBinEmitter);

        if ("l3daily".equals(targetFormat)) {
            final double mjd = extractMJDFromFilename(inputPath);

            // create lut of fires by row and column
            final HashMap<Integer, Integer> fireIndex = createFiresLUT(frpArrays, numFires, columns);

            // create observation variable sequence
            int[] variableIndex = createVariableIndex(binningContext);

            // pixel loop
            int count = 0;
            int variableOffset = getSensorOffset(platformNumber);
            Index index = frpArrays[FRP_VARIABLES.flags.ordinal()].getIndex();
            for (int row = 0; row < rows; ++row) {
                ObservationImpl[] observations = new ObservationImpl[columns];
                for (int col = 0; col < columns; ++col) {
                    // construct observation
                    double lat = geodeticArrays[GEODETIC_VARIABLES.latitude_in.ordinal()].getInt(index.set(row, col)) * 1e-6;
                    double lon = geodeticArrays[GEODETIC_VARIABLES.longitude_in.ordinal()].getInt(index.set(row, col)) * 1e-6;
                    int flags = frpArrays[FRP_VARIABLES.flags.ordinal()].getInt(index.set(row, col));
                    Integer fire = fireIndex.get(row * columns + col);
                    float frpMwir = Float.NaN;
                    if (fire != null) {
                        double area = frpArrays[FRP_VARIABLES.IFOV_area.ordinal()].getDouble(fire);
                        if (area > 0.0) {
                            frpMwir = (float) frpArrays[FRP_VARIABLES.FRP_MWIR.ordinal()].getDouble(fire);
                            if (frpMwir <= 0.0) {
                                frpMwir = Float.NaN;
                            } else {
                                ++count;
                            }
                        }
                    }
                    //long binIndex = binningContext.getPlanetaryGrid().getBinIndex(lat, lon);
                    // aggregate contributions based on flags
                    float[] values = new float[5];
                    values[variableIndex[0]] = 1;
                    values[variableIndex[1]] = (flags & 32) != 0 ? 1 : 0;  // frp_cloud
                    values[variableIndex[2]] = (flags & 6) != 0 ? 1 : 0;  // l1b_water | frp_water
                    values[variableIndex[3]] = !Float.isNaN(frpMwir) ? 1 : 0;
                    values[variableIndex[4]] = frpMwir;
                    observations[col] = new ObservationImpl(lat, lon, mjd, values);
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
        }
    }

    static int getSensorOffset(int platformNumber) {
        if (platformNumber == 1) {
            return 0;
        } else if (platformNumber == 2) {
            return 10;
        }

        throw new IllegalArgumentException("Unknown Sentinel platform");
    }

    static int[] createVariableIndex(BinningContext binningContext) {
        final VariableContext variableContext = binningContext.getVariableContext();
        return new int[]{
                variableContext.getVariableIndex("s3a_day_pixel"),
                variableContext.getVariableIndex("s3a_day_cloud"),
                variableContext.getVariableIndex("s3a_day_water"),
                variableContext.getVariableIndex("s3a_day_fire"),
                variableContext.getVariableIndex("s3a_day_frp"),
                variableContext.getVariableIndex("s3a_night_pixel"),
                variableContext.getVariableIndex("s3a_night_cloud"),
                variableContext.getVariableIndex("s3a_night_water"),
                variableContext.getVariableIndex("s3a_night_fire"),
                variableContext.getVariableIndex("s3a_night_frp"),
                variableContext.getVariableIndex("s3b_day_pixel"),
                variableContext.getVariableIndex("s3b_day_cloud"),
                variableContext.getVariableIndex("s3b_day_water"),
                variableContext.getVariableIndex("s3b_day_fire"),
                variableContext.getVariableIndex("s3b_day_frp"),
                variableContext.getVariableIndex("s3b_night_pixel"),
                variableContext.getVariableIndex("s3b_night_cloud"),
                variableContext.getVariableIndex("s3b_night_water"),
                variableContext.getVariableIndex("s3b_night_fire"),
                variableContext.getVariableIndex("s3b_night_frp")
        };
    }

    private int writeL2MonthlyBin(Context context, float platformNumber, Array[] frpArrays, int numFires) throws IOException, InterruptedException {
        //System.out.print("Time\tLatitude\tLongitude\tRow\tColumn\tFRP_MIR\tFRP_SWIR\tAREA\tday_flag\tf1_flag\tPlatform\tConfidence\n");
        int count = 0;
        final Index flagsIdx = frpArrays[FRP_VARIABLES.flags.ordinal()].getIndex();
        for (int i = 0; i < numFires; ++i) {
            // filter
            final long time = frpArrays[FRP_VARIABLES.time.ordinal()].getLong(i);
            final double latitude = frpArrays[FRP_VARIABLES.latitude.ordinal()].getDouble(i);
            final double longitude = frpArrays[FRP_VARIABLES.longitude.ordinal()].getDouble(i);
            final int row = frpArrays[FRP_VARIABLES.j.ordinal()].getInt(i);
            final int col = frpArrays[FRP_VARIABLES.i.ordinal()].getShort(i);
            final double frpMwir = frpArrays[FRP_VARIABLES.FRP_MWIR.ordinal()].getDouble(i);
            final double frpSwir = frpArrays[FRP_VARIABLES.FRP_SWIR.ordinal()].getDouble(i);
            final double area = frpArrays[FRP_VARIABLES.IFOV_area.ordinal()].getDouble(i);
            final int flags = frpArrays[FRP_VARIABLES.flags.ordinal()].getInt(flagsIdx.set(row, col));
            final int used_channel = frpArrays[FRP_VARIABLES.used_channel.ordinal()].getInt(i);
            final double confidence = frpArrays[FRP_VARIABLES.confidence.ordinal()].getDouble(i);
            if (area <= 0.0) {
                LOG.info("skipping empty area record at time " + time);
                continue;
            }
            if (frpMwir <= 0.0) {
                LOG.info("skipping non-MWIR record at time " + time);
                continue;
            }
/*
            System.out.print(ISO_DATE_FORMAT.format(new Date(time / 1000 + THIRTY_YEARS)));
            System.out.print('\t');
            System.out.print(String.format("%8.5f", latitude));
            System.out.print('\t');
            System.out.print(String.format("%8.5f", longitude));
            System.out.print('\t');
            System.out.print(String.format("%d", row));
            System.out.print('\t');
            System.out.print(String.format("%d", col));
            System.out.print('\t');
            System.out.print(String.format("%f", frpMwir));
            System.out.print('\t');
            System.out.print(String.format("%f", frpSwir));
            System.out.print('\t');
            System.out.print(String.format("%f", area));
            System.out.print('\t');
            System.out.print(String.format("%d", flags));
            System.out.print('\t');
            System.out.print(String.format("%d", used_channel));
            System.out.print('\t');
            System.out.print(String.format("%s", platformNumber == 1 ? "S3A" : platformNumber == 2 ? "S3B" : "unknown"));
            System.out.print('\t');
            System.out.print(String.format("%f", confidence));
            System.out.print('\n');
*/
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
            featureValues[FRP_VARIABLES.FRP_SWIR.ordinal()] = (float) frpSwir;
            featureValues[FRP_VARIABLES.IFOV_area.ordinal()] = (float) area;
            featureValues[FRP_VARIABLES.flags.ordinal()] = (float) ((flags & 64) != 0 ? 1 : 0);
            featureValues[FRP_VARIABLES.used_channel.ordinal()] = (float) used_channel;
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

    private NetcdfFile openNetcdfFile(File inputFile) throws IOException {
        final NetcdfFile netcdfFile = NetcdfFileOpener.open(inputFile.getPath());
        if (netcdfFile == null) {
            throw new IOException("Failed to open file " + inputFile.getPath());
        }
        return netcdfFile;
    }

    enum FRP_VARIABLES {
        time,
        latitude,
        longitude,
        j,
        i,
        FRP_MWIR,
        FRP_SWIR,
        IFOV_area,
        flags,
        used_channel,
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

        public SpatialBinEmitter(Context context) {
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
