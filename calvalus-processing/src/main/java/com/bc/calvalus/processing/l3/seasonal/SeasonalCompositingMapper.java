package com.bc.calvalus.processing.l3.seasonal;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.glevel.MultiLevelImage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.common.BandMathsOp;
import org.esa.snap.core.util.ImageUtils;

import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.bc.calvalus.processing.l3.seasonal.SeasonalCompositingReducer.MERIS_BANDS;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class SeasonalCompositingMapper extends Mapper<NullWritable, NullWritable, IntWritable, BandTileWritable> {

    protected static final Logger LOG = CalvalusLogger.getLogger();

    public static final int STATUS_CLOUD_SHADOW = 5;
    static int NUM_INDEXES = 8;

    private static final SimpleDateFormat DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd");
    private static final DateFormat COMPACT_DATE_FORMAT = DateUtils.createDateFormat("yyyyMMdd");
    private static final DateFormat YEAR_FORMAT = DateUtils.createDateFormat("yyyy");
    //                                                     ESACCI-LC-L3-SR-MERIS-300m-P7D-h36v08-20100101-v1.0.nc
    private static final String SR_FILENAME_FORMAT      = "ESACCI-LC-L3-SR-%s-P%dD-h%02dv%02d-%s-%s.nc";
    private static final String SR_FILENAME_FORMAT_MSI  = "ESACCI-LC-L3-SR-%s-P%dD-h%03dv%03d-%s-%s.nc";
    //                                                                OLCI-L3-P1D-h21v06-20180729-1.7.3.nc
    private static final String SR_FILENAME_FORMAT_OLCI            = "%s-P%dD-h%02dv%02d-%s-%s.nc";
    private static final Pattern SR_FILENAME_PATTERN =
            Pattern.compile("(?:ESACCI-LC-L3-SR-|)([^-]*-[^-]*)-[^-]*-h([0-9]*)v([0-9]*)-........-([^-]*).nc");

    public static final int DEBUG_X = 6700 % (64800/18);
    public static final int DEBUG_Y = 3700 % (64800/18);
    public static final int DEBUG_X2 = 6700 % (64800/18);
    public static final int DEBUG_Y2 = 5500 % (64800/18);
    public static final boolean DEBUG = false;
    private static final float EPS = 1.0E-6f;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        // determine path of some input, weeks
        // /calvalus/eodata/MERIS_SR_FR/v1.0/2010/2010-01-01/ESACCI-LC-L3-SR-MERIS-300m-P7D-h36v08-20100101-v1.0.nc
        final Path someTilePath = ((FileSplit) context.getInputSplit()).getPath();
        final Matcher matcher = SR_FILENAME_PATTERN.matcher(someTilePath.getName());
        if (! matcher.matches()) {
            throw new IllegalArgumentException("file name " + someTilePath.getName() + " does not match pattern " + SR_FILENAME_PATTERN.pattern());
        }
        final String sensorAndResolution = matcher.group(1);
        final int tileColumn = Integer.parseInt(matcher.group(2), 10);
        final int tileRow = Integer.parseInt(matcher.group(3), 10);
        //final String version = matcher.group(4);
        final boolean isMsi = sensorAndResolution.startsWith("MSI");
        final boolean isOlci = sensorAndResolution.startsWith("OLCI");

        final Configuration conf = context.getConfiguration();
        final int mosaicHeight;
        final boolean withMaxNdvi;
        final boolean withBestPixels;
        try {
            final BinningConfig binningConfig = BinningConfig.fromXml(conf.get(JobConfigNames.CALVALUS_L3_PARAMETERS));
            mosaicHeight = binningConfig.getNumRows();
            withMaxNdvi = binningConfig.getAggregatorConfigs() != null && binningConfig.getAggregatorConfigs().length > 0 && "ON_MAX_SET".equals(binningConfig.getAggregatorConfigs()[0].getName());
            withBestPixels = binningConfig.getAggregatorConfigs() != null && binningConfig.getAggregatorConfigs().length > 0 && "PERCENTILE".equals(binningConfig.getAggregatorConfigs()[0].getName());
            LOG.info("compositing by " + (withMaxNdvi ? "maximum NDVI" : withBestPixels ? "best pixels" : "averaging"));
        } catch (BindingException e) {
            throw new IllegalArgumentException("L3 parameters not well formed: " + e.getMessage() + " in " + conf.get(JobConfigNames.CALVALUS_L3_PARAMETERS));
        }
        final int numTileRows = isMsi ? 180 : isOlci ? 18 : 36;
        final int numMicroTiles = isMsi ? 5 : isOlci ? 2 : 1;
        final int tileSize = mosaicHeight / numTileRows;  // 64800 / 36 = 1800, 16200 / 36 = 450, 972000 / 72 = 13500
        final int microTileSize = tileSize / numMicroTiles;

        final Path srRootDir = isMsi ? someTilePath.getParent().getParent() : someTilePath.getParent().getParent().getParent();  // TODO check for other sensors
        final FileSystem fs = someTilePath.getFileSystem(conf);

        final Date start = getDate(conf, JobConfigNames.CALVALUS_MIN_DATE);
        final Date stop = getDate(conf, JobConfigNames.CALVALUS_MAX_DATE);
        final Calendar startCalendar = DateUtils.createCalendar();
        final Calendar stopCalendar = DateUtils.createCalendar();
        startCalendar.setTime(start);
        stopCalendar.setTime(stop);
        stopCalendar.set(Calendar.YEAR, startCalendar.get(Calendar.YEAR));
        if (stopCalendar.before(startCalendar)) {
            stopCalendar.add(Calendar.YEAR, 1);
        }

        // determine target bands and indexes
        String[] sensorBands = sensorBandsOf(sensorAndResolution);
        String[] targetBands = targetBandsOf(conf, sensorBands);
        int[] targetBandIndex = new int[17];  // desired band for the seasonal composite, as index to sensorBands
        int[] sourceBandIndex = new int[20];  // corresponding required band of the L3 product, as index to product
        int numTargetBands = 3;
        int numSourceBands = 6;
            for (int j = 0; j < 3; ++j) {
                targetBandIndex[j] = j;
            }
            for (int i = 0; i < 6; ++i) {
                sourceBandIndex[i] = i;
            }
            for (int j = 3; j < sensorBands.length && numTargetBands < targetBands.length; ++j) {
                if (sensorBands[j].equals(targetBands[numTargetBands])) {  // sequence is important
                    targetBandIndex[numTargetBands++] = j;
                    sourceBandIndex[numSourceBands++] = sourceBandIndexOf(sensorAndResolution, j);
                }
            }
        final int b1BandIndex = isMsi ? 6 - 1 + 1 : isOlci ? 6-1+2 : 6 - 1 + 1;   // TODO only valid for S2 and PROBA
        final int b3BandIndex = isMsi ? 6 - 1 + 2 : isOlci ? 6-1+5 : 6 - 1 + 4; // TODO only valid for S2 and PROBA
        final int b11BandIndex = isMsi ? 6 - 1 + 9 : isOlci ? 6-1+13 : 6 - 1 + 3; // TODO only valid for S2 and PROBA
        final int ndviBandIndex = numSourceBands - 1;

        // initialise aggregation variables array, status, statusCount, count, bands 1-10,12-14, ndvi
        final List<MultiLevelImage[]> bandImages = new ArrayList<>();
        final List<Product> products = new ArrayList<>();
        final int daysPerWeek = isMsi ? 10 : isOlci ? 1 : 7;
        // loop over weeks
        for (Date week = start; ! stop.before(week); week = nextWeek(week, startCalendar, stopCalendar, daysPerWeek)) {

            // determine and read input tile for the week
            final String weekFileName = String.format(isMsi  ? SR_FILENAME_FORMAT_MSI :
                                                      isOlci ? SR_FILENAME_FORMAT_OLCI :
                                                               SR_FILENAME_FORMAT,
                                                      sensorAndResolution, daysPerWeek, tileColumn, tileRow, COMPACT_DATE_FORMAT.format(week), /*version*/"*");
            Path path = new Path(new Path(isMsi  ? srRootDir :
                                          isOlci ? new Path(srRootDir, String.format("h%02dv%02d", tileColumn, tileRow)) :
                                                   new Path(srRootDir, YEAR_FORMAT.format(week)),
                                          DATE_FORMAT.format(week)),
                                       weekFileName);
            FileStatus[] fileStatuses = fs.globStatus(path);
            if (fileStatuses == null || fileStatuses.length == 0) {
                LOG.info("skipping non-existing period " + path);
                continue;
            }
            path = fileStatuses[0].getPath();
            LOG.info("aggregating period " + weekFileName);

            Product product = readProduct(conf, fs, path);
            if (weekFileName.startsWith("OLCI")) {
                product = sdrToSr(product);
            }
            final MultiLevelImage[] bandImage = new MultiLevelImage[numSourceBands];
            for (int b = 0; b < numSourceBands; ++b) {
                if (week == start)  {
                    LOG.info("source band " + product.getBandAt(sourceBandIndex[b]).getName() + " index " + sourceBandIndex[b]);
                }
                bandImage[b] = product.getBandAt(sourceBandIndex[b]).getGeophysicalImage();
            }
            bandImages.add(bandImage);
            products.add(product);
        }

        float[][] accu = new float[numTargetBands][microTileSize * microTileSize];
        final short[][] bandDataB = new short[numSourceBands][];
        final short[][] bandDataS = new short[numSourceBands][];
        final float[][] bandDataF = new float[numSourceBands][];

        // prepare arrays for and in case of ndvi/mndvi for best pixels aggregation
        float[][] ndviSum = null;
        float[][] ndviSqrSum = null;
        int[][] ndviCount = null;
        float[][] ndxiMax = null;
        float[] ndviMean = null;
        float[] ndviSdev = null;
        if (withBestPixels) {
            ndviSum = new float[NUM_INDEXES][microTileSize*microTileSize];
            ndviSqrSum = new float[NUM_INDEXES][microTileSize*microTileSize];
            ndviCount = new int[NUM_INDEXES][microTileSize*microTileSize];
            ndxiMax = new float[3][microTileSize*microTileSize];
            ndviMean = new float[microTileSize*microTileSize];
            ndviSdev = new float[microTileSize*microTileSize];
        }

        for (int microTileY = 0; microTileY < numMicroTiles; ++microTileY) {
            for (int microTileX = 0; microTileX < numMicroTiles; ++microTileX) {
                final Rectangle microTileArea = new Rectangle(microTileX * microTileSize, microTileY * microTileSize, microTileSize, microTileSize);
                for (int b = 0; b < numTargetBands; b++) {
                    Arrays.fill(accu[b], 0.0f);
                }
                // count status and average ndvi (or ndwi) for each status separately
                if (withBestPixels) {
                    for (int j=0; j<NUM_INDEXES; ++j) {
                        Arrays.fill(ndviSum[j], 0.0f);
                        Arrays.fill(ndviSqrSum[j], 0.0f);
                        Arrays.fill(ndviCount[j], 0);
                    }
                    Arrays.fill(ndxiMax[0], -1.0f);
                    Arrays.fill(ndxiMax[1], -1.0f);
                    Arrays.fill(ndxiMax[2], 1.0f);
                    for (MultiLevelImage[] bandImage : bandImages) {
                        for (int b = 0; b < numSourceBands; b++) {
                            if (b == 0) {
                                bandDataB[b] = (short[]) ImageUtils.getPrimitiveArray(bandImage[b].getData(microTileArea).getDataBuffer());
                            } else if (b < 6) {
                                bandDataS[b] = (short[]) ImageUtils.getPrimitiveArray(bandImage[b].getData(microTileArea).getDataBuffer());
                            } else {
                                bandDataF[b] = (float[]) ImageUtils.getPrimitiveArray(bandImage[b].getData(microTileArea).getDataBuffer());
                            }
                        }
                        // pixel loop
                        for (int i = 0; i < microTileSize * microTileSize; ++i) {
                            final int state = (int) bandDataB[0][i];
                            final int index = index(state);
                            if (index < 0 || containsNan(bandDataF, numTargetBands, i)) {
                                continue;
                            }
                            ndviCount[index][i]++;
                            switch (state) {
                                case 1:
                                case 15:
                                case 12:
                                case 11:
                                case 5:
                                    float ndvi = bandDataF[ndviBandIndex][i];
                                    ndviSum[index][i] += ndvi;
                                    ndviSqrSum[index][i] += ndvi * ndvi;
                                    if (ndvi > ndxiMax[0][i]) {
                                        ndxiMax[0][i] = ndvi;
                                    }
                                    if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X, DEBUG_Y)) {
                                        LOG.info("x=" + DEBUG_X + " y=" + DEBUG_Y + " i=" + i + " state=" + state + " index=" + index + " count=" + ndviCount[index][i] + " ndvi=" + ndvi);
                                    }
                                    if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X2, DEBUG_Y2)) {
                                        LOG.info("x=" + DEBUG_X2 + " y=" + DEBUG_Y2 + " i=" + i + " state=" + state + " index=" + index + " count=" + ndviCount[index][i] + " ndvi=" + ndvi);
                                    }
                                    break;
                                case 2:
                                case 3:  // TODO TBC whether to use water index for snow as well
                                    float ndwi = (bandDataF[b11BandIndex][i] - bandDataF[b3BandIndex][i]) / (bandDataF[b11BandIndex][i] + bandDataF[b3BandIndex][i]);
                                    ndviSum[index][i] += ndwi;
                                    ndviSqrSum[index][i] += ndwi * ndwi;
                                    if (ndwi > ndxiMax[1][i]) {
                                        ndxiMax[1][i] = ndwi;
                                    }
                                    if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X, DEBUG_Y)) {
                                        LOG.info("x=" + DEBUG_X + " y=" + DEBUG_Y + " i=" + i + " state=" + state + " index=" + index + " count=" + ndviCount[index][i] + " ndwi=" + ndwi);
                                    }
                                    if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X2, DEBUG_Y2)) {
                                        LOG.info("x=" + DEBUG_X2 + " y=" + DEBUG_Y2 + " i=" + i + " state=" + state + " index=" + index + " count=" + ndviCount[index][i] + " ndwi=" + ndwi);
                                    }
                                    break;
                                case 4:
                                case 14:
                                    float b1Value = bandDataF[b1BandIndex][i];
                                    if (b1Value < ndxiMax[2][i]) {
                                        ndxiMax[2][i] = b1Value;
                                    }
                            }
                        }
                    }
                    // we have counted the different stati over time, and summed up ndvi per status,
                    // ... determine majority/priority
                    for (int i = 0; i < microTileSize * microTileSize; ++i) {
                        int state = majorityPriorityStatusOf(ndviCount, i);
                        int index = index(state);
                        if (index < 0) {
                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X, DEBUG_Y)) {
                                LOG.info("x=" + DEBUG_X + " y=" + DEBUG_Y + " i=" + i + " ignored for ndvi, state=" + state + " index=" + index);
                            }
                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X2, DEBUG_Y2)) {
                                LOG.info("x=" + DEBUG_X2 + " y=" + DEBUG_Y2 + " i=" + i + " ignored for ndvi, state=" + state + " index=" + index);
                            }
                            continue;
                        }
                        accu[0][i] = state;
                        if (index < 7) {
                            ndviMean[i] = ndviSum[index][i] / ndviCount[index][i];
                            ndviSdev[i] = (float) Math.sqrt(ndviSqrSum[index][i] / ndviCount[index][i] - ndviMean[i] * ndviMean[i]);
                        } else {  // cloud or temporal cloud
                            ndviMean[i] = Float.NaN;
                            ndviSdev[i] = Float.NaN;
                        }
                        if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X, DEBUG_Y)) {
                            LOG.info("x=" + DEBUG_X + " y=" + DEBUG_Y + " i=" + i + " majostate=" + state + " majoindex=" + index + " mean=" + ndviMean[i] + " sigma=" + ndviSdev[i]);
                        }
                        if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X2, DEBUG_Y2)) {
                            LOG.info("x=" + DEBUG_X2 + " y=" + DEBUG_Y2 + " i=" + i + " majostate=" + state + " majoindex=" + index + " mean=" + ndviMean[i] + " sigma=" + ndviSdev[i]);
                        }
                    }
                    // we have determined the majority/priority status and ndvi mean and sigma for it
                }
                for (MultiLevelImage[] bandImage : bandImages) {
                    for (int b = 0; b < numSourceBands; b++) {
                        if (b == 0) {
                            bandDataB[b] = (short[]) ImageUtils.getPrimitiveArray(bandImage[b].getData(microTileArea).getDataBuffer());
                        } else if (b < 6) {
                            bandDataS[b] = (short[]) ImageUtils.getPrimitiveArray(bandImage[b].getData(microTileArea).getDataBuffer());
                        } else {
                            bandDataF[b] = (float[]) ImageUtils.getPrimitiveArray(bandImage[b].getData(microTileArea).getDataBuffer());
                        }
                    }

                    // pixel loop
                    for (int i = 0; i < microTileSize * microTileSize; ++i) {

                        // aggregate pixel-wise using aggregation rules
                        final int state = (int) bandDataB[0][i];
                        if (state <= 0) {
                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X, DEBUG_Y)) {
                                LOG.info("x=" + DEBUG_X + " y=" + DEBUG_Y + " i=" + i + " ignored, state=" + state);
                            }
                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X2, DEBUG_Y2)) {
                                LOG.info("x=" + DEBUG_X2 + " y=" + DEBUG_Y2 + " i=" + i + " ignored, state=" + state);
                            }
                            continue;
                        }
                        if (containsNan(bandDataF, numTargetBands, i)) {
                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X, DEBUG_Y)) {
                                LOG.info("x=" + DEBUG_X + " y=" + DEBUG_Y + " i=" + i + " ignored, value NaN, state=" + state);
                            }
                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X2, DEBUG_Y2)) {
                                LOG.info("x=" + DEBUG_X2 + " y=" + DEBUG_Y2 + " i=" + i + " ignored, value NaN, state=" + state);
                            }
                            continue;
                        }
                        if (withBestPixels) {
                            accu[2][i] += count(bandDataS, i);
                            if (state == accu[0][i]) {
                                switch (state) {
                                    case 1:
                                    case 15:
                                    case 12:
                                    case 11:
                                    case 5:
                                        // if (bandDataF[ndviBandIndex][i] >= ndxiMax[0][i] - ndviSdev[i] - EPS) {
                                        if (bandDataF[ndviBandIndex][i] >= ndviMean[i] - ndviSdev[i] - EPS && bandDataF[ndviBandIndex][i] <= ndviMean[i] + ndviSdev[i] + EPS) {
                                            final int stateCount = count(state == 1 ? state : STATUS_CLOUD_SHADOW, bandDataS, i);  // cloud shadow count abused for dark, bright, haze
                                            accu[1][i] += stateCount;
                                            for (int b = 3; b < numTargetBands; ++b) {
                                                accu[b][i] += stateCount * bandDataF[b + 3][i];
                                            }
                                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X, DEBUG_Y)) {
                                                LOG.info("x=" + DEBUG_X + " y=" + DEBUG_Y + " i=" + i + " state=" + state + " count=" + stateCount + " ndvi=" + bandDataF[ndviBandIndex][i] + " high. aggregated");
                                            }
                                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X2, DEBUG_Y2)) {
                                                LOG.info("x=" + DEBUG_X2 + " y=" + DEBUG_Y2 + " i=" + i + " state=" + state + " count=" + stateCount + " ndvi=" + bandDataF[ndviBandIndex][i] + " high. aggregated");
                                            }
                                        } else {
                                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X, DEBUG_Y)) {
                                                LOG.info("x=" + DEBUG_X + " y=" + DEBUG_Y + " i=" + i + " state=" + state + " ndvi=" + bandDataF[ndviBandIndex][i] + " low or high. skipped");
                                            }
                                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X2, DEBUG_Y2)) {
                                                LOG.info("x=" + DEBUG_X2 + " y=" + DEBUG_Y2 + " i=" + i + " state=" + state + " ndvi=" + bandDataF[ndviBandIndex][i] + " low or high. skipped");
                                            }
                                        }
                                        break;
                                    case 2:
                                    case 3:  // TODO TBC whether to use water index for snow as well
                                        float ndwi = (bandDataF[b11BandIndex][i] - bandDataF[b3BandIndex][i]) / (bandDataF[b11BandIndex][i] + bandDataF[b3BandIndex][i]);
                                        //if (ndwi >= ndxiMax[1][i] - ndviSdev[i] - EPS) {
                                        if (ndwi >= ndviMean[i] - ndviSdev[i] - EPS && ndwi <= ndviMean[i] + ndviSdev[i] + EPS) {
                                            final int stateCount = count(state, bandDataS, i);
                                            accu[1][i] += stateCount;
                                            for (int b = 3; b < numTargetBands; ++b) {
                                                accu[b][i] += stateCount * bandDataF[b + 3][i];
                                            }
                                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X, DEBUG_Y)) {
                                                LOG.info("x=" + DEBUG_X + " y=" + DEBUG_Y + " i=" + i + " state=" + state + " count=" + stateCount + " ndwi=" + ndwi + " high. aggregated");
                                            }
                                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X2, DEBUG_Y2)) {
                                                LOG.info("x=" + DEBUG_X2 + " y=" + DEBUG_Y2 + " i=" + i + " state=" + state + " count=" + stateCount + " ndwi=" + ndwi + " high. aggregated");
                                            }
                                        } else {
                                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X, DEBUG_Y)) {
                                                LOG.info("x=" + DEBUG_X + " y=" + DEBUG_Y + " i=" + i + " state=" + state + " ndwi=" + ndwi + " low.or high skipped");
                                            }
                                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X2, DEBUG_Y2)) {
                                                LOG.info("x=" + DEBUG_X2 + " y=" + DEBUG_Y2 + " i=" + i + " state=" + state + " ndwi=" + ndwi + " low or high. skipped");
                                            }
                                        }
                                        break;
                                    case 4:
                                    case 14:
                                        final int stateCount = count(state, bandDataS, i);
                                        accu[1][i] += stateCount;
                                        break;
                                }
                            }
                        } else if (state == accu[0][i]) {
                            // same state as before, aggregate ...
                            final int stateCount = count(state, bandDataS, i);
                            accu[1][i] += stateCount;
                            accu[2][i] += count(bandDataS, i);
                            if (withMaxNdvi) {
                                if (bandDataF[ndviBandIndex][i] > accu[numTargetBands - 1][i]) {
                                    for (int b = 3; b < numTargetBands; ++b) {
                                        accu[b][i] = bandDataF[b + 3][i];
                                    }
                                }
                            } else {
                                for (int b = 3; b < numTargetBands; ++b) {
                                    accu[b][i] += stateCount * bandDataF[b + 3][i];
                                }
                            }
                        } else if (rank(state) > rank(accu[0][i])) {
                            // better state, e.g. land instead of snow: restart counting ...
                            final int stateCount = count(state, bandDataS, i);
                            accu[0][i] = state;
                            accu[1][i] = stateCount;
                            accu[2][i] = count(bandDataS, i);
                            if (withMaxNdvi) {
                                for (int b = 3; b < numTargetBands; ++b) {
                                    accu[b][i] = bandDataF[b + 3][i];
                                }
                            } else {
                                for (int b = 3; b < numTargetBands; ++b) {
                                    accu[b][i] = stateCount * bandDataF[b + 3][i];
                                }
                            }
                        }
                    }
                }

                // finish aggregation, divide by stateCount
                if (!withMaxNdvi) {
                    for (int i = 0; i < microTileSize * microTileSize; ++i) {
                        final float stateCount = accu[1][i];
                        for (int b = 3; b < numTargetBands; ++b) {
                            accu[b][i] /= stateCount;
                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X, DEBUG_Y)) {
                                LOG.info("x=" + DEBUG_X + " y=" + DEBUG_Y + " i=" + i + " count=" + stateCount + " b=" + b + " bandvalue=" + accu[b][i]);
                            }
                            if (DEBUG && isAtPosition(i, microTileX, microTileY, microTileSize, numMicroTiles, DEBUG_X2, DEBUG_Y2)) {
                                LOG.info("x=" + DEBUG_X2 + " y=" + DEBUG_Y2 + " i=" + i + " count=" + stateCount + " b=" + b + " bandvalue=" + accu[b][i]);
                            }
                        }
                    }
                }

                // statistics for logging
                final int[] counts = new int[16];
                for (float state : accu[0]) {
                    ++counts[rank(state)];
                }
                LOG.info((counts[14]+counts[10]+counts[9]+counts[8]) + " land, " + (counts[15]) + " water, " + counts[13] + " snow, " + counts[5] + " shadow, " + (counts[1]+counts[2]) + " cloud");
                if (counts[14]+counts[10]+counts[9]+counts[8] == 0 && counts[15] == 0 && counts[13] == 0 && counts[5] == 0 && counts[1]+counts[2] == 0) {
                    continue;
                }

                // stream results, one per band
                for (int b = 0; b < numTargetBands; ++b) {
                    // compose key from band and tile
                    final int bandAndTile = ((sensorBands.length - 3) << 27) + (targetBandIndex[b] << 22) + ((tileRow * numMicroTiles + microTileY) << 11) + (tileColumn * numMicroTiles + microTileX);
                    LOG.info("streaming band " + targetBandIndex[b] + " tile row " + (tileRow * numMicroTiles + microTileY) + " tile column " + (tileColumn * numMicroTiles + microTileX) + " key " + bandAndTile);
                    // write tile
                    final IntWritable key = new IntWritable(bandAndTile);
                    final BandTileWritable value = new BandTileWritable(accu[b]);
                    context.write(key, value);
                }
            }
        }

        for (Product product : products) {
            product.dispose();
        }
    }

    private static boolean containsNan(float[][] bandDataF, int numTargetBands, int i) {
        for (int b = 3; b < numTargetBands; ++b) {
            if (Float.isNaN(bandDataF[b + 3][i])) {
                return true;
            }
        }
        return false;
    }

    private Product sdrToSr(Product product) {
        Map<String, Object> parameters = new HashMap<>();
        BandMathsOp.BandDescriptor[] bandDescriptors = new BandMathsOp.BandDescriptor[20];

        bandDescriptors[0] = new BandMathsOp.BandDescriptor();
        bandDescriptors[0].name = "current_pixel_state";
        bandDescriptors[0].expression = "current_pixel_state";
        bandDescriptors[0].type = ProductData.TYPESTRING_INT8;

        bandDescriptors[1] = new BandMathsOp.BandDescriptor();
        bandDescriptors[1].name = "clear_land_count";
        bandDescriptors[1].expression = "current_pixel_state == 1 ? num_obs : 0";
        bandDescriptors[1].type = ProductData.TYPESTRING_INT16;

        bandDescriptors[2] = new BandMathsOp.BandDescriptor();
        bandDescriptors[2].name = "clear_water_count";
        bandDescriptors[2].expression = "current_pixel_state == 2 ? num_obs : 0";
        bandDescriptors[2].type = ProductData.TYPESTRING_INT16;

        bandDescriptors[3] = new BandMathsOp.BandDescriptor();
        bandDescriptors[3].name = "clear_snow_ice_count";
        bandDescriptors[3].expression = "current_pixel_state == 3 ? num_obs : 0";
        bandDescriptors[3].type = ProductData.TYPESTRING_INT16;

        bandDescriptors[4] = new BandMathsOp.BandDescriptor();
        bandDescriptors[4].name = "cloud_count";
        bandDescriptors[4].expression = "current_pixel_state == 4 ? num_obs : 0";
        bandDescriptors[4].type = ProductData.TYPESTRING_INT16;

        bandDescriptors[5] = new BandMathsOp.BandDescriptor();
        bandDescriptors[5].name = "cloud_shadow_count";
        bandDescriptors[5].expression = "current_pixel_state >= 5 ? num_obs : 0";
        bandDescriptors[5].type = ProductData.TYPESTRING_INT16;

        bandDescriptors[6] = new BandMathsOp.BandDescriptor();
        bandDescriptors[6].name = "sr_1_mean";
        bandDescriptors[6].expression = "sdr_1";
        bandDescriptors[6].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[7] = new BandMathsOp.BandDescriptor();
        bandDescriptors[7].name = "sr_2_mean";
        bandDescriptors[7].expression = "sdr_2";
        bandDescriptors[7].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[8] = new BandMathsOp.BandDescriptor();
        bandDescriptors[8].name = "sr_3_mean";
        bandDescriptors[8].expression = "sdr_3";
        bandDescriptors[8].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[9] = new BandMathsOp.BandDescriptor();
        bandDescriptors[9].name = "sr_4_mean";
        bandDescriptors[9].expression = "sdr_4";
        bandDescriptors[9].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[10] = new BandMathsOp.BandDescriptor();
        bandDescriptors[10].name = "sr_5_mean";
        bandDescriptors[10].expression = "sdr_5";
        bandDescriptors[10].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[11] = new BandMathsOp.BandDescriptor();
        bandDescriptors[11].name = "sr_6_mean";
        bandDescriptors[11].expression = "sdr_6";
        bandDescriptors[11].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[12] = new BandMathsOp.BandDescriptor();
        bandDescriptors[12].name = "sr_7_mean";
        bandDescriptors[12].expression = "sdr_7";
        bandDescriptors[12].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[13] = new BandMathsOp.BandDescriptor();
        bandDescriptors[13].name = "sr_8_mean";
        bandDescriptors[13].expression = "sdr_8";
        bandDescriptors[13].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[14] = new BandMathsOp.BandDescriptor();
        bandDescriptors[14].name = "sr_9_mean";
        bandDescriptors[14].expression = "sdr_9";
        bandDescriptors[14].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[15] = new BandMathsOp.BandDescriptor();
        bandDescriptors[15].name = "sr_10_mean";
        bandDescriptors[15].expression = "sdr_10";
        bandDescriptors[15].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[16] = new BandMathsOp.BandDescriptor();
        bandDescriptors[16].name = "sr_12_mean";
        bandDescriptors[16].expression = "sdr_12";
        bandDescriptors[16].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[17] = new BandMathsOp.BandDescriptor();
        bandDescriptors[17].name = "sr_13_mean";
        bandDescriptors[17].expression = "sdr_13";
        bandDescriptors[17].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[18] = new BandMathsOp.BandDescriptor();
        bandDescriptors[18].name = "sr_14_mean";
        bandDescriptors[18].expression = "sdr_14";
        bandDescriptors[18].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[19] = new BandMathsOp.BandDescriptor();
        bandDescriptors[19].name = "vegetation_index_mean";
        bandDescriptors[19].expression = "ndvi_max";
        bandDescriptors[19].type = ProductData.TYPESTRING_FLOAT32;

        parameters.put("targetBands", bandDescriptors);
        product = GPF.createProduct("BandMaths", parameters, product);
        return product;
    }

    private boolean isAtPosition(int i, int microTileX, int microTileY, int microTileSize, int numMicroTiles, int x, int y) {
        return x == microTileX * microTileSize + (i % microTileSize) && y == microTileY * microTileSize + (i / microTileSize);
    }

    private int majorityPriorityStatusOf(int[][] ndviCount, int i) {
        return (ndviCount[1][i] > 0 && ndviCount[1][i] >= ndviCount[0][i] && ndviCount[1][i] >= ndviCount[2][i]) ? 2 :
               (ndviCount[0][i] > 0 && ndviCount[0][i] >= ndviCount[2][i]) ? 1 :
               ndviCount[2][i] > 0 ? 3 :
               ndviCount[3][i] > 0 ? 15 :
               ndviCount[4][i] > 0 ? 12 :
               ndviCount[5][i] > 0 ? 11 :
               ndviCount[6][i] > 0 ? 5 :
               ndviCount[7][i] > 0 ? 4 : 0;
    }

    static int sourceBandIndexOf(String sensorAndResolution, int targetBandIndex) {
        return
            "MERIS-300m".equals(sensorAndResolution) ? 2 * targetBandIndex :  // sr_1 is source 6 target 3 etc.
            "MERIS-1000m".equals(sensorAndResolution) ? 2 * targetBandIndex :  // sr_1 is source 6 target 3 etc.
            "OLCI-L3".equals(sensorAndResolution) ? targetBandIndex + 3 :  // sr_1 is source 6 target 3 etc.
            "AVHRR-1000m".equals(sensorAndResolution) ? (targetBandIndex < 5 ? 2 * targetBandIndex : targetBandIndex + 5) :  // sr_1 is source 6 target 3, bt_3 is source 10 target 6
            "PROBAV-1000m".equals(sensorAndResolution) ? (targetBandIndex < 3 ? targetBandIndex : targetBandIndex + 3) :  // sr_1 is source 6 target 3 etc.
            "VEGETATION-1000m".equals(sensorAndResolution) ? 2 * targetBandIndex :  // sr_1 is source 6 target 3 etc.
            "PROBAV-300m".equals(sensorAndResolution) ? (targetBandIndex < 3 ? targetBandIndex : targetBandIndex + 3) :  // sr_1 is source 6 target 3 etc.
            "PROBAV-333m".equals(sensorAndResolution) ? (targetBandIndex < 3 ? targetBandIndex : targetBandIndex + 3) :  // sr_1 is source 6 target 3 etc.
            "VEGETATION-300m".equals(sensorAndResolution) ? 2 * targetBandIndex :  // sr_1 is source 6 target 3 etc.
            "MSI-20m".equals(sensorAndResolution) ? (targetBandIndex < 3 ? targetBandIndex : targetBandIndex + 3) :  // sr_1 is source 6 target 3 etc.
            "MSI-10m".equals(sensorAndResolution) ? (targetBandIndex < 3 ? targetBandIndex : targetBandIndex + 3) :  // sr_1 is source 6 target 3 etc.
            -1;
    }

    static String[] targetBandsOf(Configuration conf, String[] sensorBands) {
        final String bandConfig = conf.get("calvalus.seasonal.bands");
        if (bandConfig != null) {
            String[] configBands = bandConfig.split(",");
            String[] targetBands = new String[3+configBands.length];
            for (int j=0; j<3; ++j) {
                targetBands[j] = sensorBands[j];
            }
            for (int j=3; j<targetBands.length; ++j) {
                targetBands[j] = configBands[j-3];
            }
            return targetBands;
        } else {
            return sensorBands;
        }
    }

    static String[] sensorBandsOf(String sensorAndResolution) {
        switch (sensorAndResolution) {
            case "MERIS-300m":
            case "MERIS-1000m":
            case "OLCI-L3":
                return MERIS_BANDS;
            case "AVHRR-1000m":
                return SeasonalCompositingReducer.AVHRR_BANDS;
            case "VEGETATION-1000m":
            case "VEGETATION-300m":
            case "PROBAV-1000m":
            case "PROBAV-300m":
            case "PROBAV-333m":
                return SeasonalCompositingReducer.PROBA_BANDS;
            case "MSI-20m":
                return SeasonalCompositingReducer.MSI_BANDS;
            case "MSI-10m":
                return SeasonalCompositingReducer.AGRI_BANDS;
            default:
                throw new IllegalArgumentException("unknown sensor and resolution " + sensorAndResolution);
        }
    }

    static Product readProduct(Configuration conf, FileSystem fs, Path path) throws IOException {
        final File localFile = new File(path.getName());
        FileUtil.copy(fs, path, localFile, false, conf);
        return ProductIO.readProduct(localFile);
    }

    static Date getDate(Configuration conf, String parameterName) {
        try {
            return DATE_FORMAT.parse(conf.get(parameterName));
        } catch (ParseException e) {
            throw new IllegalArgumentException("parameter " + parameterName + " value " + conf.get(parameterName) +
                                               " does not match pattern " + DATE_FORMAT.toPattern() + ": " + e.getMessage(), e);
        }
    }

    static Date nextWeek(Date week, Calendar start, Calendar stop, int daysPerWeek) {
        GregorianCalendar c = DateUtils.createCalendar();
        c.setTime(week);
        c.add(Calendar.DATE, daysPerWeek == 7 ? lengthOfWeek(c) : daysPerWeek);
        if (c.after(stop)) {
            start.add(Calendar.YEAR, 1);
            stop.add(Calendar.YEAR, 1);
            c.setTime(start.getTime());
        }
        return c.getTime();
    }

    static Date shiftTo(Date week, int year) {
        GregorianCalendar c = DateUtils.createCalendar();
        c.setTime(week);
        c.set(Calendar.YEAR, year);
        return c.getTime();
    }

    static int lengthOfWeek(GregorianCalendar c) {
        if (c.get(Calendar.MONTH) == Calendar.FEBRUARY && c.get(Calendar.DAY_OF_MONTH) == 26 && c.isLeapYear(c.get(Calendar.YEAR))) {
            return 8;
        } else if (c.get(Calendar.MONTH) == Calendar.DECEMBER && c.get(Calendar.DAY_OF_MONTH) == 24) {
            return 8;
        }
        return 7;
    }

    static int count(short[][] bandData, int iSrc) {
        return (int) bandData[1][iSrc]
                + (int) bandData[2][iSrc]
                + (int) bandData[3][iSrc]
                + (int) bandData[4][iSrc]
                + (int) bandData[5][iSrc];
    }

    static int count(int state, short[][] bandData, int i) {
        switch (state) {
            case 1: return (int) bandData[1][i];
            case 2: return (int) bandData[2][i];
            case 3: return (int) bandData[3][i];
            case 4: return (int) bandData[4][i];
            case 5:
            case 12:
            case 11:
            case 15: return (int) bandData[5][i];
            default: return 0;
        }
    }

    static int rank(float state) {
        switch ((int) state) {
            case 2: return 15;
            case 1: return 14;
            case 3: return 13;
            case 15: return 10;  // dark
            case 12: return 9;  // bright
            case 11: return 8;  // haze
            case 5: return 5;  // shadow
            case 14: return 2;  // temporal cloud
            case 4: return 1;
            default: return 0;
        }
    }

    static int index(int state) {
        switch (state) {
            case 1: return 0;
            case 2: return 1;
            case 3: return 2;
            case 15: return 3;  // dark
            case 12: return 4;  // bright
            case 11: return 5;  // haze
            case 5: return 6;  // shadow
            case 4:
            case 14: return 7;  // cloud or temporal cloud
            default: return -1;
        }
    }
}