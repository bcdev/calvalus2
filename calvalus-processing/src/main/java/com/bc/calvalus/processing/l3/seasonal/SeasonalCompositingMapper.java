package com.bc.calvalus.processing.l3.seasonal;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.GpfUtils;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
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
import org.esa.snap.runtime.Engine;

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

    public static final int DEBUG_X = 898 % 10800;
    public static final int DEBUG_Y = 2916 % 10800;
    public static final int DEBUG_X2 = 854 % 10800;
    public static final int DEBUG_Y2 = 2910 % 10800;
    public static final boolean DEBUG = false;
    private static final float EPS = 1.0E-6f;

    short[][] bandDataB;
    short[][] bandDataS;
    float[][] bandDataF;
    float[][] ndxiSum;
    float[][] ndxiSqrSum;
    int[][] ndxiCount;
    int[][] statusCount;
    float[] ndxiMean;
    float[] ndxiSdev;
    float[][] accu;

    ProgressSplitProgressMonitor pm;

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        GpfUtils.init(context.getConfiguration());
        Engine.start();  // required here!  we do not use a ProcessorAdapter
        CalvalusLogger.restoreCalvalusLogFormatter();
        // parse input path /calvalus/eodata/MERIS_SR_FR/v1.0/2010/2010-01-01/ESACCI-LC-L3-SR-MERIS-300m-P7D-h36v08-20100101-v1.0.nc
        final Path someTilePath = ((FileSplit) context.getInputSplit()).getPath();
        final Matcher matcher = SR_FILENAME_PATTERN.matcher(someTilePath.getName());
        if (! matcher.matches()) {
            throw new IllegalArgumentException("file name " + someTilePath.getName() + " does not match pattern " + SR_FILENAME_PATTERN.pattern());
        }
        final String sensorAndResolution = matcher.group(1);
        final int tileColumn = Integer.parseInt(matcher.group(2), 10);
        final int tileRow = Integer.parseInt(matcher.group(3), 10);
        final boolean isMsi = sensorAndResolution.startsWith("MSI");
        final boolean isOlci = sensorAndResolution.startsWith("OLCI");

        // read configuration
        final Configuration conf = context.getConfiguration();
        final Date start = getDate(conf, JobConfigNames.CALVALUS_MIN_DATE);
        final Date stop = getDate(conf, JobConfigNames.CALVALUS_MAX_DATE);
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

        final FileSystem fs = someTilePath.getFileSystem(conf);
        final Path srRootDir = isMsi ? someTilePath.getParent().getParent() : someTilePath.getParent().getParent().getParent();  // TODO check for other sensors

        // sensor-dependent resolution parameters
        final int numTileRows = isMsi ? 180 : isOlci ? 18 : 36;
        final int numMicroTiles = isMsi ? 5 : isOlci ? 2 : 1;
        final int tileSize = mosaicHeight / numTileRows;  // 64800 / 36 = 1800, 16200 / 36 = 450, 972000 / 72 = 13500
        final int microTileSize = tileSize / numMicroTiles;
        final int daysPerWeek = isMsi ? 10 : isOlci ? 1 : 7;

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
        final int b3BandIndex = isMsi ? 6 - 1 + 2 : isOlci ? 6-1+5 : 6 - 1 + 4; // TODO only valid for S2 and PROBA
        final int b11BandIndex = isMsi ? 6 - 1 + 9 : isOlci ? 6-1+13 : 6 - 1 + 3; // TODO only valid for S2 and PROBA
        final int ndviBandIndex = numSourceBands - 1;

        // 2 for opening a product, 1 for majority status per time step, 5 for aggregation per time step, 4 for streaming
        final int numTimeSteps = (int) ((stop.getTime() - start.getTime()) / 86400 / 1000 / daysPerWeek);
        final int totalWork = numMicroTiles * numMicroTiles * (numTimeSteps * (2 + 1 + 5) + 4);
        pm = new ProgressSplitProgressMonitor(context);
        pm.beginTask("seasonalCompositing", totalWork);

        // initialise aggregation variables array, status, statusCount, count, bands 1-10,12-14, ndvi
        final List<MultiLevelImage[]> bandImages = new ArrayList<>();
        final List<Product> products = new ArrayList<>();
        long timestamp0 = System.currentTimeMillis();
        findAndOpenInputProducts(sensorAndResolution, tileColumn, tileRow, start, stop, daysPerWeek,
                                 srRootDir, conf, fs, isMsi, isOlci, numSourceBands, sourceBandIndex,
                                 products, bandImages);
        LOG.info("inputs determined in " + (System.currentTimeMillis()-timestamp0) + " millis");
        // pre-allocate arrays for band values per data type, for best pixels aggregation, and for transfer to reducer
        bandDataB = new short[numSourceBands][];
        bandDataS = new short[numSourceBands][];
        bandDataF = new float[numSourceBands][];
        if (withBestPixels) {
            ndxiSum = new float[NUM_INDEXES][microTileSize*microTileSize];
            ndxiSqrSum = new float[NUM_INDEXES][microTileSize*microTileSize];
            ndxiCount = new int[NUM_INDEXES][microTileSize*microTileSize];
            statusCount = new int[NUM_INDEXES][microTileSize*microTileSize];
            ndxiMean = new float[microTileSize*microTileSize];
            ndxiSdev = new float[microTileSize*microTileSize];
        }
        accu = new float[numTargetBands][microTileSize * microTileSize];

        // micro tile loop
        LOG.info("processing " + (numMicroTiles*numMicroTiles) + " micro tiles ...");
        for (int microTileY = 0; microTileY < numMicroTiles; ++microTileY) {
            for (int microTileX = 0; microTileX < numMicroTiles; ++microTileX) {
                long timestamp1 = System.currentTimeMillis();
                long timestamp3;
                final Rectangle microTileArea = new Rectangle(microTileX * microTileSize, microTileY * microTileSize, microTileSize, microTileSize);
                clearAccu(numTargetBands, accu);
                if (withBestPixels) {

                    determineMajorityStatus(bandImages, microTileArea,
                                            b3BandIndex, b11BandIndex, ndviBandIndex,
                                            ndxiMean, ndxiSdev, accu);
                    long timestamp2 = System.currentTimeMillis();
                    LOG.info("majority status determined in " + (timestamp2-timestamp1) + " millis");
                    aggregateBestPixels(bandImages, microTileArea,
                                        b3BandIndex, b11BandIndex, ndviBandIndex,
                                        numSourceBands, numTargetBands,
                                        accu);
                    divideByCount(microTileArea, numTargetBands, accu);
                    timestamp3 = System.currentTimeMillis();
                    LOG.info("best pixels aggregated in " + (timestamp3-timestamp2) + " millis");
                } else if (withMaxNdvi) {
                    aggregateByMaxNdvi(bandImages, microTileArea, ndviBandIndex, numSourceBands, numTargetBands,
                                       accu);
                    timestamp3 = System.currentTimeMillis();
                    LOG.info("max ndvi aggregated in " + (timestamp3-timestamp1) + " millis");
                } else {
                    aggregateByStatusRank(bandImages, microTileArea, numSourceBands, numTargetBands,
                                          accu);
                    divideByCount(microTileArea, numTargetBands, accu);
                    timestamp3 = System.currentTimeMillis();
                    LOG.info("average aggregated in " + (timestamp3-timestamp1) + " millis");
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
                    //LOG.info("streaming band " + targetBandIndex[b] + " tile row " + (tileRow * numMicroTiles + microTileY) + " tile column " + (tileColumn * numMicroTiles + microTileX) + " key " + bandAndTile);
                    // write tile
                    final IntWritable key = new IntWritable(bandAndTile);
                    final BandTileWritable value = new BandTileWritable(accu[b]);
                    context.write(key, value);
                }
                long timestamp4 = System.currentTimeMillis();
                LOG.info("result streamed in " + (timestamp4-timestamp3) + " millis");
                pm.worked(4);
            }
        }
        for (Product product : products) {
            product.dispose();
        }
    }

    private void findAndOpenInputProducts(String sensorAndResolution, int tileColumn, int tileRow,
                                          Date start, Date stop, int daysPerWeek,
                                          Path srRootDir, Configuration conf, FileSystem fs,
                                          boolean isMsi, boolean isOlci, int numSourceBands, int[] sourceBandIndex,
                                          List<Product> products, List<MultiLevelImage[]> bandImages) throws IOException {
        final Calendar startCalendar = DateUtils.createCalendar();
        final Calendar stopCalendar = DateUtils.createCalendar();
        startCalendar.setTime(start);
        stopCalendar.setTime(stop);
        stopCalendar.set(Calendar.YEAR, startCalendar.get(Calendar.YEAR));
        if (stopCalendar.before(startCalendar)) {
            stopCalendar.add(Calendar.YEAR, 1);
        }

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
            if (isOlci) {
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
            pm.worked(2);
        }
    }

    private void determineMajorityStatus(List<MultiLevelImage[]> bandImages, Rectangle microTileArea,
                                         int b3BandIndex, int b11BandIndex, int ndviBandIndex,
                                         float[] ndxiMean, float[] ndxiSdev, float[][] accu) {
        clearNdxi();
        for (MultiLevelImage[] bandImage : bandImages) {
            readStatusBand(bandImage, microTileArea, bandDataB);
            readNdviNdwiBands(bandImage, b3BandIndex, b11BandIndex, ndviBandIndex, microTileArea, bandDataF);
            // pixel loop
            for (int i = 0; i < microTileArea.height * microTileArea.width; ++i) {
                final int state = (int) bandDataB[0][i];
                final int index = index(state);
                if (index >= 0) {
                    statusCount[index][i]++;
                }
                if (index >= 0 && ! Float.isNaN(bandDataF[ndviBandIndex][i]) && ! Float.isNaN(bandDataF[b3BandIndex][i]) && ! Float.isNaN(bandDataF[b11BandIndex][i])) {
                    switch (state) {
                        case 1:
                        case 15:
                        case 12:
                        case 11:
                        case 5:
                            float ndvi = bandDataF[ndviBandIndex][i];
                            ndxiSum[index][i] += ndvi;
                            ndxiSqrSum[index][i] += ndvi * ndvi;
                            ndxiCount[index][i]++;
                            break;
                        case 2:
                        case 3:  // TODO TBC whether to use water index for snow as well
                            float ndwi = (bandDataF[b11BandIndex][i] - bandDataF[b3BandIndex][i]) / (bandDataF[b11BandIndex][i] + bandDataF[b3BandIndex][i]);
                            ndxiSum[index][i] += ndwi;
                            ndxiSqrSum[index][i] += ndwi * ndwi;
                            ndxiCount[index][i]++;
                            break;
                    }
                }
                //traceState(state, index, i, statusCount, microTileArea);
            }
            pm.worked(1);
        }
        for (int i = 0; i < microTileArea.height * microTileArea.width; ++i) {
            int state = majorityPriorityStatusOf(statusCount, i);
            int index = index(state);
            if (index >= 0) {
                accu[0][i] = state;
            }
            if (index >= 0 && index < 7 && ndxiCount[index][i] > 0) {
                ndxiMean[i] = ndxiSum[index][i] / ndxiCount[index][i];
                ndxiSdev[i] = (float) Math.sqrt(ndxiSqrSum[index][i] / ndxiCount[index][i] - ndxiMean[i] * ndxiMean[i]);
            } else {  // invalid or cloud or temporal cloud
                ndxiMean[i] = Float.NaN;
                ndxiSdev[i] = Float.NaN;
            }
            //traceMajoState(state, index, i, ndxiCount, ndxiMean, ndxiSdev, microTileArea);
        }
    }

    private void aggregateBestPixels(List<MultiLevelImage[]> bandImages, Rectangle microTileArea,
                                     int b3BandIndex, int b11BandIndex, int ndviBandIndex,
                                     int numSourceBands, int numTargetBands,
                                     float[][] accu) {
        // product loop
        for (MultiLevelImage[] bandImage : bandImages) {
            readSourceBands(bandImage, microTileArea, numSourceBands, bandDataB, bandDataS, bandDataF);
            // pixel loop
            for (int i = 0; i < microTileArea.height * microTileArea.width; ++i) {
                final int state = (int) bandDataB[0][i];
                if (state > 0) {
                    accu[2][i] += count(bandDataS, i);
                    if (state == accu[0][i] && ! containsNan(bandDataF, numTargetBands, i)) {
                        switch (state) {
                            case 1:
                            case 15:
                            case 12:
                            case 11:
                            case 5:
                                float ndvi = bandDataF[ndviBandIndex][i];
                                if (ndvi >= ndxiMean[i] - ndxiSdev[i] - EPS && ndvi <= ndxiMean[i] + ndxiSdev[i] + EPS) {
                                    final int stateCount = count(state == 1 ? state : STATUS_CLOUD_SHADOW, bandDataS, i);  // cloud shadow count abused for dark, bright, haze
                                    accu[1][i] += stateCount;
                                    for (int b = 3; b < numTargetBands; ++b) {
                                        accu[b][i] += stateCount * bandDataF[b + 3][i];
                                    }
                                    //traceAggregation(state, i, stateCount, ndvi, bandDataF, microTileArea);
                                }
                                break;
                            case 2:
                            case 3:  // TODO TBC whether to use water index for snow as well
                                float ndwi = (bandDataF[b11BandIndex][i] - bandDataF[b3BandIndex][i]) / (bandDataF[b11BandIndex][i] + bandDataF[b3BandIndex][i]);
                                if (ndwi >= ndxiMean[i] - ndxiSdev[i] - EPS && ndwi <= ndxiMean[i] + ndxiSdev[i] + EPS) {
                                    final int stateCount = count(state, bandDataS, i);
                                    accu[1][i] += stateCount;
                                    for (int b = 3; b < numTargetBands; ++b) {
                                        accu[b][i] += stateCount * bandDataF[b + 3][i];
                                    }
                                    //traceAggregation(state, i, stateCount, ndwi, bandDataF, microTileArea);
                                }
                                break;
                            case 4:
                            case 14:
                                final int stateCount = count(state, bandDataS, i);
                                accu[1][i] += stateCount;
                                for (int b = 3; b < numTargetBands; ++b) {
                                    accu[b][i] += stateCount * bandDataF[b + 3][i];  // we may have processed under clouds
                                }
                                //traceAggregation(state, i, stateCount, bandDataF[10+3][i], bandDataF, microTileArea);
                                break;
                        }
                    }
                }
            }
            pm.worked(5);
        }
    }

    private void aggregateByMaxNdvi(List<MultiLevelImage[]> bandImages, Rectangle microTileArea,
                                    int ndviBandIndex, int numSourceBands, int numTargetBands,
                                    float[][] accu) {
        for (MultiLevelImage[] bandImage : bandImages) {
            readSourceBands(bandImage, microTileArea, numSourceBands, bandDataB, bandDataS, bandDataF);
            // pixel loop
            for (int i = 0; i < microTileArea.height * microTileArea.width; ++i) {
                // aggregate pixel-wise using aggregation rules
                final int state = (int) bandDataB[0][i];
                if (state > 0 && ! containsNan(bandDataF, numTargetBands, i)) {
                    if (state == accu[0][i]) {
                        // same state as before, aggregate ...
                        final int stateCount = count(state, bandDataS, i);
                        accu[1][i] += stateCount;
                        accu[2][i] += count(bandDataS, i);
                        if (bandDataF[ndviBandIndex][i] > accu[numTargetBands - 1][i]) {
                            for (int b = 3; b < numTargetBands; ++b) {
                                accu[b][i] = bandDataF[b + 3][i];
                            }
                        }
                    } else if (rank(state) > rank(accu[0][i])) {
                        // better state, e.g. land instead of snow: restart counting ...
                        final int stateCount = count(state, bandDataS, i);
                        accu[0][i] = state;
                        accu[1][i] = stateCount;
                        accu[2][i] = count(bandDataS, i);
                        for (int b = 3; b < numTargetBands; ++b) {
                            accu[b][i] = bandDataF[b + 3][i];
                        }
                    }
                }
            }
            pm.worked(6);
        }
    }

    private void aggregateByStatusRank(List<MultiLevelImage[]> bandImages, Rectangle microTileArea,
                                       int numSourceBands, int numTargetBands,
                                       float[][] accu) {
        for (MultiLevelImage[] bandImage : bandImages) {
            readSourceBands(bandImage, microTileArea, numSourceBands, bandDataB, bandDataS, bandDataF);
            // pixel loop
            for (int i = 0; i < microTileArea.height * microTileArea.width; ++i) {
                final int state = (int) bandDataB[0][i];
                if (state > 0 && ! containsNan(bandDataF, numTargetBands, i)) {
                    if (state == accu[0][i]) {
                        // same state as before, aggregate ...
                        final int stateCount = count(state, bandDataS, i);
                        accu[1][i] += stateCount;
                        accu[2][i] += count(bandDataS, i);
                        for (int b = 3; b < numTargetBands; ++b) {
                            accu[b][i] += stateCount * bandDataF[b + 3][i];
                        }
                    } else if (rank(state) > rank(accu[0][i])) {
                        // better state, e.g. land instead of snow: restart counting ...
                        final int stateCount = count(state, bandDataS, i);
                        accu[0][i] = state;
                        accu[1][i] = stateCount;
                        accu[2][i] = count(bandDataS, i);
                        for (int b = 3; b < numTargetBands; ++b) {
                            accu[b][i] = stateCount * bandDataF[b + 3][i];
                        }
                    }
                }
            }
            pm.worked(6);
        }
    }

    private void divideByCount(Rectangle microTileArea, int numTargetBands, float[][] accu) {
        for (int i = 0; i < microTileArea.height * microTileArea.width; ++i) {
            final float stateCount = accu[1][i];
            for (int b = 3; b < numTargetBands; ++b) {
                if (stateCount > 0) {
                    accu[b][i] /= stateCount;
                } else {
                    accu[b][i] = Float.NaN;
                }
                //traceValue(i, stateCount, b, accu, microTileArea);
            }
        }
    }


    private void clearAccu(int numTargetBands, float[][] accu) {
        for (int b = 0; b < numTargetBands; b++) {
            Arrays.fill(accu[b], 0.0f);
        }
    }

    private void clearNdxi() {
        for (int j=0; j<NUM_INDEXES; ++j) {
            Arrays.fill(ndxiSum[j], 0.0f);
            Arrays.fill(ndxiSqrSum[j], 0.0f);
            Arrays.fill(ndxiCount[j], 0);
            Arrays.fill(statusCount[j], 0);
        }
    }

    static Product readProduct(Configuration conf, FileSystem fs, Path path) throws IOException {
        final File localFile = new File(path.getName());
        FileUtil.copy(fs, path, localFile, false, conf);
        return ProductIO.readProduct(localFile);
    }

    private void readSourceBands(MultiLevelImage[] bandImage, Rectangle microTileArea, int numSourceBands, short[][] bandDataB, short[][] bandDataS, float[][] bandDataF) {
        for (int b = 0; b < numSourceBands; b++) {
            if (b == 0) {
                bandDataB[b] = (short[]) ImageUtils.getPrimitiveArray(bandImage[b].getData(microTileArea).getDataBuffer());
            } else if (b < 6) {
                bandDataS[b] = (short[]) ImageUtils.getPrimitiveArray(bandImage[b].getData(microTileArea).getDataBuffer());
            } else {
                bandDataF[b] = (float[]) ImageUtils.getPrimitiveArray(bandImage[b].getData(microTileArea).getDataBuffer());
            }
        }
    }

    private void readStatusBand(MultiLevelImage[] bandImage, Rectangle microTileArea, short[][] bandDataB) {
        bandDataB[0] = (short[]) ImageUtils.getPrimitiveArray(bandImage[0].getData(microTileArea).getDataBuffer());
    }

    private void readNdviNdwiBands(MultiLevelImage[] bandImage, int b3BandIndex, int b11BandIndex, int ndviBandIndex, Rectangle microTileArea, float[][] bandDataF) {
        bandDataF[b3BandIndex] = (float[]) ImageUtils.getPrimitiveArray(bandImage[b3BandIndex].getData(microTileArea).getDataBuffer());
        bandDataF[b11BandIndex] = (float[]) ImageUtils.getPrimitiveArray(bandImage[b11BandIndex].getData(microTileArea).getDataBuffer());
        bandDataF[ndviBandIndex] = (float[]) ImageUtils.getPrimitiveArray(bandImage[ndviBandIndex].getData(microTileArea).getDataBuffer());
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
        bandDescriptors[1].expression = "current_pixel_state == 1 and !nan(ndvi_max) ? 1 : 0";
        bandDescriptors[1].type = ProductData.TYPESTRING_INT16;

        bandDescriptors[2] = new BandMathsOp.BandDescriptor();
        bandDescriptors[2].name = "clear_water_count";
        bandDescriptors[2].expression = "current_pixel_state == 2 and !nan(ndvi_max) ? 1 : 0";
        bandDescriptors[2].type = ProductData.TYPESTRING_INT16;

        bandDescriptors[3] = new BandMathsOp.BandDescriptor();
        bandDescriptors[3].name = "clear_snow_ice_count";
        bandDescriptors[3].expression = "current_pixel_state == 3 and !nan(ndvi_max) ? 1 : 0";
        bandDescriptors[3].type = ProductData.TYPESTRING_INT16;

        bandDescriptors[4] = new BandMathsOp.BandDescriptor();
        bandDescriptors[4].name = "cloud_count";
        bandDescriptors[4].expression = "current_pixel_state == 4 and !nan(ndvi_max) ? 1 : 0";
        bandDescriptors[4].type = ProductData.TYPESTRING_INT16;

        bandDescriptors[5] = new BandMathsOp.BandDescriptor();
        bandDescriptors[5].name = "cloud_shadow_count";
        bandDescriptors[5].expression = "current_pixel_state >= 5 and !nan(ndvi_max) ? 1 : 0";
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

    private boolean isAtPosition(int i, Rectangle microTileArea, int x, int y) {
        return x == microTileArea.x + (i % microTileArea.width) && y == microTileArea.y + (i / microTileArea.width);
    }

    private void traceState(int state, int index, int i, int[][] stateCount, Rectangle microTileArea) {
        if (DEBUG && isAtPosition(i, microTileArea, DEBUG_X, DEBUG_Y)) {
            LOG.info("x=" + DEBUG_X + " y=" + DEBUG_Y + " i=" + i + " state=" + state + " index=" + index + " count=" + (index >= 0 ? stateCount[index][i] : -1));
        }
        if (DEBUG && isAtPosition(i, microTileArea, DEBUG_X2, DEBUG_Y2)) {
            LOG.info("x=" + DEBUG_X2 + " y=" + DEBUG_Y2 + " i=" + i + " state=" + state + " index=" + index + " count=" + (index >= 0 ? stateCount[index][i] : -1));
        }
    }

    private void traceMajoState(int state, int index, int i, int[][] stateCount, float[] ndviMean, float[] ndviSdev, Rectangle microTileArea) {
        if (DEBUG && isAtPosition(i, microTileArea, DEBUG_X, DEBUG_Y)) {
            LOG.info("x=" + DEBUG_X + " y=" + DEBUG_Y + " i=" + i + " majostate=" + state + " index=" + index + " count=" + stateCount[index][i] + " mean=" + ndviMean[i] + " sigma=" + ndviSdev[i]);
        }
        if (DEBUG && isAtPosition(i, microTileArea, DEBUG_X2, DEBUG_Y2)) {
            LOG.info("x=" + DEBUG_X2 + " y=" + DEBUG_Y2 + " i=" + i + " majostate=" + state + " index=" + index + " count=" + stateCount[index][i] + " mean=" + ndviMean[i] + " sigma=" + ndviSdev[i]);
        }
    }

    private void traceAggregation(int state, int i, int stateCount, float ndxi, float[][] bandDataF,Rectangle microTileArea) {
        if (DEBUG && isAtPosition(i, microTileArea, DEBUG_X, DEBUG_Y)) {
            LOG.info("x=" + DEBUG_X + " y=" + DEBUG_Y + " i=" + i + " state=" + state + " count=" + stateCount + " ndxi=" + ndxi + " band value " + bandDataF[6][i] + " aggregated");
        }
        if (DEBUG && isAtPosition(i, microTileArea, DEBUG_X2, DEBUG_Y2)) {
            LOG.info("x=" + DEBUG_X2 + " y=" + DEBUG_Y2 + " i=" + i + " state=" + state + " count=" + stateCount + " ndxi=" + ndxi + " band value " + bandDataF[6][i] + " aggregated");
        }
    }

    private void traceValue(int i, float stateCount, int b, float[][] accu, Rectangle microTileArea) {
        if (DEBUG && isAtPosition(i, microTileArea, DEBUG_X, DEBUG_Y)) {
            LOG.info("x=" + DEBUG_X + " y=" + DEBUG_Y + " i=" + i + " count=" + stateCount + " b=" + b + " bandvalue=" + accu[b][i]);
        }
        if (DEBUG && isAtPosition(i, microTileArea, DEBUG_X2, DEBUG_Y2)) {
            LOG.info("x=" + DEBUG_X2 + " y=" + DEBUG_Y2 + " i=" + i + " count=" + stateCount + " b=" + b + " bandvalue=" + accu[b][i]);
        }
    }
}