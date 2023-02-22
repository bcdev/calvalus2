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
import static com.bc.calvalus.processing.l3.seasonal.SeasonalCompositingReducer.SYN_BANDS;

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
    //                                                               SYN-L3-P1D-h21v06-20210101-S3B-1.0.nc
    private static final String SR_FILENAME_FORMAT_SYN            = "%s-P%dD-h%02dv%02d-%s-%s.nc";
    private static final Pattern SR_FILENAME_PATTERN =
            Pattern.compile("(?:ESACCI-LC-L3-SR-|)([^-]*-[^-]*)-[^-]*-h([0-9]*)v([0-9]*)-........-(.*).nc");

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
    float[][] ndxiMean;
    float[][] ndxiSdev;
    float[][] accu;

    float srThreshold;

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
        final boolean isSyn = sensorAndResolution.startsWith("SYN");

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
            srThreshold = Float.parseFloat(System.getProperty("calvalus.compositing.srthreshold", "NaN"));
            LOG.info("using sr threshold " + srThreshold + " (numbers lower than that are considered NaN, switched off if NaN itself");
            LOG.info("testing isNaN: " + isNaN(-0.6f) + " " + isNaN(-0.4f) + " " + isNaN(Float.NaN));
        } catch (BindingException e) {
            throw new IllegalArgumentException("L3 parameters not well formed: " + e.getMessage() + " in " + conf.get(JobConfigNames.CALVALUS_L3_PARAMETERS));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cannot parse value of calvalus.compositing.srthreshold '" +
                                               System.getProperty("calvalus.compositing.srthreshold") + "' as a number: " + e);
        }

        final FileSystem fs = someTilePath.getFileSystem(conf);
        final Path srRootDir = isMsi ? someTilePath.getParent().getParent() : someTilePath.getParent().getParent().getParent();  // TODO check for other sensors

        // sensor-dependent resolution parameters
        final int numTileRows = isMsi ? 180 : isOlci ? 18 : isSyn ? 18 : 36;
        final int numMicroTiles = isMsi ? 5 : isOlci ? 2 : isSyn ? 2 : 1;
        final int tileSize = mosaicHeight / numTileRows;  // 64800 / 36 = 1800, 16200 / 36 = 450, 972000 / 72 = 13500
        final int microTileSize = tileSize / numMicroTiles;
        final int daysPerWeek = isMsi ? 10 : isOlci ? 1 : isSyn ? 1 : 7;

        // determine target bands and indexes
        String[] sensorBands = sensorBandsOf(sensorAndResolution);
        // String[] targetBands = targetBandsOf(conf, sensorBands);  numSourceBands must be complete. Else xxxBandIndex may be wrong
        String[] targetBands = sensorBands;
        int[] targetBandIndex = new int[26];  // desired band for the seasonal composite, as index to sensorBands
        int[] sourceBandIndex = new int[29];  // corresponding required band of the L3 product, as index to product
        int numTargetBands = (isSyn ? 5 : 3);
        int numSourceBands = 6;
        for (int j = 0; j < (isSyn ? 5 : 3); ++j) {
            targetBandIndex[j] = j;
        }
        for (int i = 0; i < 6; ++i) {
            sourceBandIndex[i] = i;
        }
        for (int j = (isSyn ? 5 : 3); j < sensorBands.length && numTargetBands < targetBands.length; ++j) {
            if (sensorBands[j].equals(targetBands[numTargetBands])) {  // sequence is important
                targetBandIndex[numTargetBands++] = j;
                sourceBandIndex[numSourceBands++] = sourceBandIndexOf(sensorAndResolution, j);
            }
        }
        // for SYN the OLCI NDWI is computed from Oa17 and Oa08
        final int b3BandIndex = isMsi ? 6 - 1 + 2 : isOlci ? 6-1+5 : isSyn ? 6-1+8 : 6 - 1 + 4;    // green 560 nm
        final int b11BandIndex = isMsi ? 6 - 1 + 9 : isOlci ? 6-1+13 : isSyn ? 6-1+13: 6 - 1 + 3;  // swir-1 1610 nm
        final int ndviBandIndex = numSourceBands - 1;
        final int sl01BandIndex = isSyn ? 6-1+16 : -1;

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
                                 srRootDir, conf, fs, isMsi, isOlci, isSyn, numSourceBands, sourceBandIndex,
                                 products, bandImages);
        LOG.info("inputs determined in " + (System.currentTimeMillis()-timestamp0) + " millis");
        // pre-allocate arrays for band values per data type, for best pixels aggregation, and for transfer to reducer
        bandDataB = new short[numSourceBands][];
        bandDataS = new short[numSourceBands][];
        bandDataF = new float[numSourceBands][];
        if (withBestPixels) {
            statusCount = new int[NUM_INDEXES][microTileSize*microTileSize];
            ndxiSum = new float[(isSyn ? 3 : 1) * NUM_INDEXES][microTileSize*microTileSize];
            ndxiSqrSum = new float[(isSyn ? 3 : 1) * NUM_INDEXES][microTileSize*microTileSize];
            ndxiCount = new int[(isSyn ? 3 : 1) * NUM_INDEXES][microTileSize*microTileSize];
            ndxiMean = new float[(isSyn ? 3 : 1)][microTileSize*microTileSize];
            ndxiSdev = new float[(isSyn ? 3 : 1)][microTileSize*microTileSize];
        }
        accu = new float[isSyn ? numTargetBands + 2 : numTargetBands][microTileSize * microTileSize];

        // micro tile loop
        LOG.info("processing " + (numMicroTiles*numMicroTiles) + " micro tiles ...");
        for (int microTileY = 0; microTileY < numMicroTiles; ++microTileY) {
            for (int microTileX = 0; microTileX < numMicroTiles; ++microTileX) {
                long timestamp1 = System.currentTimeMillis();
                long timestamp3;
                final Rectangle microTileArea = new Rectangle(microTileX * microTileSize, microTileY * microTileSize, microTileSize, microTileSize);
                clearAccu(numTargetBands, accu);
                if (withBestPixels) {
                    clearNdxi(isSyn ? 3 : 1);
                    determineMajorityStatus(bandImages, microTileArea, isSyn,
                                            sl01BandIndex, b3BandIndex, b11BandIndex, ndviBandIndex,
                                            ndxiMean, ndxiSdev, accu);
                    long timestamp2 = System.currentTimeMillis();
                    LOG.info("majority status determined in " + (timestamp2-timestamp1) + " millis");
                    aggregateBestPixels(bandImages, microTileArea, isSyn,
                                        b3BandIndex, b11BandIndex, ndviBandIndex,
                                        sl01BandIndex, numSourceBands, numTargetBands,
                                        accu);
                    divideByCount(microTileArea, numTargetBands, isSyn, sl01BandIndex, accu);
                    timestamp3 = System.currentTimeMillis();
                    LOG.info("best pixels aggregated in " + (timestamp3-timestamp2) + " millis");
                } else if (withMaxNdvi) {
                    aggregateByMaxNdvi(bandImages, microTileArea, ndviBandIndex, numSourceBands, numTargetBands,
                                       accu);
                    timestamp3 = System.currentTimeMillis();
                    LOG.info("max ndvi aggregated in " + (timestamp3-timestamp1) + " millis");
                } else {
                    aggregateByStatusRank(bandImages, microTileArea, ndviBandIndex, numSourceBands, numTargetBands,
                                          accu);
                    divideByCount(microTileArea, numTargetBands, false, sl01BandIndex, accu);
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
                    //final int bandAndTile = ((sensorBands.length - 3) << 27) + (targetBandIndex[b] << 22) + ((tileRow * numMicroTiles + microTileY) << 11) + (tileColumn * numMicroTiles + microTileX);
                    final int bandAndTile = ((sensorBands.length - (isSyn ? 1 : 3)) << 26) + (targetBandIndex[b] << 21) + ((tileRow * numMicroTiles + microTileY) << 11) + (tileColumn * numMicroTiles + microTileX);
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
                                          boolean isMsi, boolean isOlci, boolean isSyn, int numSourceBands, int[] sourceBandIndex,
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
                                                      isSyn  ? SR_FILENAME_FORMAT_SYN :
                                                               SR_FILENAME_FORMAT,
                                                      sensorAndResolution, daysPerWeek, tileColumn, tileRow, COMPACT_DATE_FORMAT.format(week), /*version*/"*");
            Path path = new Path(new Path(isMsi  ? srRootDir :
                                          isOlci ? new Path(srRootDir, String.format("h%02dv%02d", tileColumn, tileRow)) :
                                          isSyn  ? new Path(new Path(srRootDir.getParent(),
                                                                     srRootDir.getName().replace("s3a", "s3?")
                                                                                        .replace("s3b", "s3?")),
                                                            String.format("h%02dv%02d", tileColumn, tileRow)) :
                                                   new Path(srRootDir, YEAR_FORMAT.format(week)),
                                          DATE_FORMAT.format(week)),
                                       weekFileName);
            FileStatus[] fileStatuses = fs.globStatus(path);
            if (fileStatuses == null || fileStatuses.length == 0) {
                LOG.info("skipping non-existing period " + path);
                continue;
            }
            for (int numProds = 0 ; numProds < fileStatuses.length; numProds++) {
                path = fileStatuses[numProds].getPath();
                LOG.info("aggregating " + weekFileName);
                Product product = readProduct(conf, fs, path);
                if (isOlci) {
                    product = sdrToSr(product);
                } else if (isSyn) {
                    product = sdrToSrSyn(product);
                }
                final MultiLevelImage[] bandImage = new MultiLevelImage[numSourceBands];
                for (int b = 0; b < numSourceBands; ++b) {
                    if (week == start) {
                        LOG.info("source band " + product.getBandAt(sourceBandIndex[b]).getName() + " index " + sourceBandIndex[b]);
                    }
                    bandImage[b] = product.getBandAt(sourceBandIndex[b]).getGeophysicalImage();
                }
                bandImages.add(bandImage);
                products.add(product);
            }
            pm.worked(2);
        }
    }

    boolean isNaN(float f) {
        return Float.isNaN(f) || f < srThreshold;
    }

    private void determineMajorityStatus(List<MultiLevelImage[]> bandImages, Rectangle microTileArea, boolean isSyn,
                                         int sl01BandIndex, int b3BandIndex, int b11BandIndex, int ndviBandIndex,
                                         float[][] ndxiMean, float[][] ndxiSdev, float[][] accu) {
        for (MultiLevelImage[] bandImage : bandImages) {
            readStatusBand(bandImage, microTileArea, bandDataB);
            readNdviNdwiBands(bandImage, b3BandIndex, b11BandIndex, ndviBandIndex, isSyn, sl01BandIndex, microTileArea, bandDataF);
            // pixel loop
            for (int i = 0; i < microTileArea.height * microTileArea.width; ++i) {
                final int state = (int) bandDataB[0][i];
                final int index = index(state);
                if (index >= 0) {
                    statusCount[index][i]++;
                }
                if (index >= 0) {
                    switch (state) {
                        case 1:
                        case 15:
                        case 12:
                        case 11:
                        case 5:
                            if (! Float.isNaN(bandDataF[ndviBandIndex][i])) {
                                float ndvi = bandDataF[ndviBandIndex][i];
                                ndxiSum[index][i] += ndvi;
                                ndxiSqrSum[index][i] += ndvi * ndvi;
                                ndxiCount[index][i]++;
                            }
                            if (isSyn && ! isNaN(bandDataF[sl01BandIndex+1][i]) && ! isNaN(bandDataF[sl01BandIndex+2][i])) {
                                float ndxi = ndxiOf(bandDataF[sl01BandIndex+2][i], bandDataF[sl01BandIndex+1][i]);
                                ndxiSum[NUM_INDEXES + index][i] += ndxi;
                                ndxiSqrSum[NUM_INDEXES + index][i] += ndxi * ndxi;
                                ndxiCount[NUM_INDEXES + index][i]++;
                            }
                            if (isSyn && ! isNaN(bandDataF[sl01BandIndex+3][i]) && ! isNaN(bandDataF[sl01BandIndex+4][i])) {
                                float ndxi = ndxiOf(bandDataF[sl01BandIndex+4][i], bandDataF[sl01BandIndex+3][i]);
                                ndxiSum[2 * NUM_INDEXES + index][i] += ndxi;
                                ndxiSqrSum[2 * NUM_INDEXES + index][i] += ndxi * ndxi;
                                ndxiCount[2 * NUM_INDEXES + index][i]++;
                            }
                            break;
                        case 2:
                        case 3:  // TODO TBC whether to use water index for snow as well
                            if (! isNaN(bandDataF[b3BandIndex][i]) && ! isNaN(bandDataF[b11BandIndex][i])) {
                                float ndwi = ndxiOf(bandDataF[b11BandIndex][i], bandDataF[b3BandIndex][i]);
                                ndxiSum[index][i] += ndwi;
                                ndxiSqrSum[index][i] += ndwi * ndwi;
                                ndxiCount[index][i]++;
                            }
                            // we use the same normalised measures as for land as we do not have proper NDWI in one of the SLSTR groups
                            if (isSyn && ! isNaN(bandDataF[sl01BandIndex+1][i]) && ! isNaN(bandDataF[sl01BandIndex+2][i])) {
                                float ndxi = ndxiOf(bandDataF[sl01BandIndex+2][i], bandDataF[sl01BandIndex+1][i]);
                                ndxiSum[NUM_INDEXES + index][i] += ndxi;
                                ndxiSqrSum[NUM_INDEXES + index][i] += ndxi * ndxi;
                                ndxiCount[NUM_INDEXES + index][i]++;
                            }
                            if (isSyn && ! isNaN(bandDataF[sl01BandIndex+3][i]) && ! isNaN(bandDataF[sl01BandIndex+4][i])) {
                                float ndxi = ndxiOf(bandDataF[sl01BandIndex+4][i], bandDataF[sl01BandIndex+3][i]);
                                ndxiSum[2 * NUM_INDEXES + index][i] += ndxi;
                                ndxiSqrSum[2 * NUM_INDEXES + index][i] += ndxi * ndxi;
                                ndxiCount[2 * NUM_INDEXES + index][i]++;
                            }
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
            if (index >= 0 && index < 7) {
                if (ndxiCount[index][i] > 0) {
                    ndxiMean[0][i] = ndxiSum[index][i] / ndxiCount[index][i];
                    ndxiSdev[0][i] = sdevOf(ndxiSqrSum[index][i], ndxiMean[0][i], ndxiCount[index][i]);
                } else {
                    ndxiMean[0][i] = Float.NaN;
                    ndxiSdev[0][i] = Float.NaN;
                }
                if (isSyn) {
                    if (ndxiCount[NUM_INDEXES + index][i] > 0) {
                        ndxiMean[1][i] = ndxiSum[NUM_INDEXES + index][i] / ndxiCount[NUM_INDEXES + index][i];
                        ndxiSdev[1][i] = sdevOf(ndxiSqrSum[NUM_INDEXES + index][i], ndxiMean[1][i], ndxiCount[NUM_INDEXES + index][i]);
                    } else {
                        ndxiMean[1][i] = Float.NaN;
                        ndxiSdev[1][i] = Float.NaN;
                    }
                    if (ndxiCount[2 * NUM_INDEXES + index][i] > 0) {
                        ndxiMean[2][i] = ndxiSum[2 * NUM_INDEXES + index][i] / ndxiCount[2 * NUM_INDEXES + index][i];
                        ndxiSdev[2][i] = sdevOf(ndxiSqrSum[2 * NUM_INDEXES + index][i], ndxiMean[2][i], ndxiCount[2 * NUM_INDEXES + index][i]);
                    } else {
                        ndxiMean[2][i] = Float.NaN;
                        ndxiSdev[2][i] = Float.NaN;
                    }
                }
            } else {  // invalid or cloud or temporal cloud
                ndxiMean[0][i] = Float.NaN;
                ndxiSdev[0][i] = Float.NaN;
                if (isSyn) {
                    ndxiMean[1][i] = Float.NaN;
                    ndxiSdev[1][i] = Float.NaN;
                    ndxiMean[2][i] = Float.NaN;
                    ndxiSdev[2][i] = Float.NaN;
                }
            }
            //traceMajoState(state, index, i, ndxiCount, ndxiMean, ndxiSdev, microTileArea);
        }
    }

    private void aggregateBestPixels(List<MultiLevelImage[]> bandImages, Rectangle microTileArea, boolean isSyn,
                                     int b3BandIndex, int b11BandIndex, int ndviBandIndex, int sl01BandIndex,
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
                    if (state == accu[0][i]) {
                        switch (state) {
                            case 1:
                            case 15:
                            case 12:
                            case 11:
                            case 5:
                                if (isSyn) {
                                    if (! containsNan(bandDataF, 6, sl01BandIndex, ndviBandIndex, i)) {
                                        float ndvi = bandDataF[ndviBandIndex][i];
                                        if (within1Sigma(ndvi, ndxiMean[0][i], ndxiSdev[0][i])) {
                                            final int stateCount = count(state == 1 ? state : STATUS_CLOUD_SHADOW, bandDataS, i);  // cloud shadow count abused for dark, bright, haze
                                            accu[1][i] += stateCount;
                                            for (int b = 5; b < numTargetBands; ++b) {
                                                final int bs = b + 1;
                                                if (bs < sl01BandIndex || bs > sl01BandIndex + 4) {
                                                    accu[b][i] += stateCount * bandDataF[bs][i];
                                                }
                                            }
                                        }
                                    }
                                    if (! containsNan(bandDataF, sl01BandIndex, sl01BandIndex+3, ndviBandIndex, i)) {
                                        float ndxi123 = ndxiOf(bandDataF[sl01BandIndex + 2][i], bandDataF[sl01BandIndex + 1][i]);
                                        if (within1Sigma(ndxi123, ndxiMean[1][i], ndxiSdev[1][i])) {
                                            final int stateCount = count(state == 1 ? state : STATUS_CLOUD_SHADOW, bandDataS, i);  // cloud shadow count abused for dark, bright, haze
                                            accu[3][i] += stateCount;
                                            for (int bs = sl01BandIndex; bs < sl01BandIndex + 3; ++bs) {
                                                final int b = bs - 1;
                                                accu[b][i] += stateCount * bandDataF[bs][i];
                                            }
                                        }
                                    }
                                    if (! containsNan(bandDataF, sl01BandIndex+3, sl01BandIndex+5, ndviBandIndex, i)) {
                                        float ndxi56 = ndxiOf(bandDataF[sl01BandIndex + 4][i], bandDataF[sl01BandIndex + 3][i]);
                                        if (within1Sigma(ndxi56, ndxiMean[2][i], ndxiSdev[2][i])) {
                                            final int stateCount = count(state == 1 ? state : STATUS_CLOUD_SHADOW, bandDataS, i);  // cloud shadow count abused for dark, bright, haze
                                            accu[4][i] += stateCount;
                                            for (int bs = sl01BandIndex + 3; bs < sl01BandIndex + 5; ++bs) {
                                                final int b = bs - 1;
                                                accu[b][i] += stateCount * bandDataF[bs][i];
                                            }
                                        }
                                    }
                                } else {
                                    if (! containsNan(bandDataF, numTargetBands, ndviBandIndex, i)) {
                                        float ndvi = bandDataF[ndviBandIndex][i];
                                        if (within1Sigma(ndvi, ndxiMean[0][i], ndxiSdev[0][i])) {
                                            final int stateCount = count(state == 1 ? state : STATUS_CLOUD_SHADOW, bandDataS, i);  // cloud shadow count abused for dark, bright, haze
                                            accu[1][i] += stateCount;
                                            for (int b = 3; b < numTargetBands; ++b) {
                                                accu[b][i] += stateCount * bandDataF[b + 3][i];
                                            }
                                            //traceAggregation(state, i, stateCount, ndvi, bandDataF, microTileArea);
                                        }
                                    }
                                }
                                break;
                            case 2:
                            case 3:  // TODO TBC whether to use water index for snow as well
                                if (isSyn) {
                                    if (! containsNan(bandDataF, 6, sl01BandIndex, ndviBandIndex, i)) {
                                        float ndwi = ndxiOf(bandDataF[b11BandIndex][i], bandDataF[b3BandIndex][i]);
                                        if (within1Sigma(ndwi, ndxiMean[0][i], ndxiSdev[0][i])) {
                                            final int stateCount = count(state, bandDataS, i);
                                            accu[1][i] += stateCount;
                                            for (int b = 5; b < numTargetBands; ++b) {
                                                final int bs = b + 1;
                                                if (bs < sl01BandIndex || bs > sl01BandIndex + 4) {
                                                    accu[b][i] += stateCount * bandDataF[bs][i];
                                                }
                                            }
                                            //traceAggregation(state, i, stateCount, ndwi, bandDataF, microTileArea);
                                        }
                                    }
                                    if (! containsNan(bandDataF, sl01BandIndex, sl01BandIndex+3, ndviBandIndex, i)) {
                                        float ndxi123 = ndxiOf(bandDataF[sl01BandIndex + 2][i], bandDataF[sl01BandIndex + 1][i]);
                                        if (within1Sigma(ndxi123, ndxiMean[1][i], ndxiSdev[1][i])) {
                                            final int stateCount = count(state, bandDataS, i);
                                            accu[3][i] += stateCount;
                                            for (int bs = sl01BandIndex; bs < sl01BandIndex + 3; ++bs) {
                                                final int b = bs - 1;
                                                accu[b][i] += stateCount * bandDataF[bs][i];
                                            }
                                        }
                                    }
                                    if (! containsNan(bandDataF, sl01BandIndex+3, sl01BandIndex+5, ndviBandIndex, i)) {
                                        float ndxi56 = ndxiOf(bandDataF[sl01BandIndex + 4][i], bandDataF[sl01BandIndex + 3][i]);
                                        if (within1Sigma(ndxi56, ndxiMean[2][i], ndxiSdev[2][i])) {
                                            final int stateCount = count(state, bandDataS, i);
                                            accu[4][i] += stateCount;
                                            for (int bs = sl01BandIndex + 3; bs < sl01BandIndex + 5; ++bs) {
                                                final int b = bs - 1;
                                                accu[b][i] += stateCount * bandDataF[bs][i];
                                            }
                                        }
                                    }
                                } else {
                                    if (! containsNan(bandDataF, numTargetBands, ndviBandIndex, i)) {
                                        float ndwi = ndxiOf(bandDataF[b11BandIndex][i], bandDataF[b3BandIndex][i]);
                                        if (within1Sigma(ndwi, ndxiMean[0][i], ndxiSdev[0][i])) {
                                            final int stateCount = count(state, bandDataS, i);
                                            accu[1][i] += stateCount;
                                            for (int b = 3; b < numTargetBands; ++b) {
                                                accu[b][i] += stateCount * bandDataF[b + 3][i];
                                            }
                                            //traceAggregation(state, i, stateCount, ndwi, bandDataF, microTileArea);
                                        }
                                    }
                                }
                                break;
                            case 4:
                            case 14:
                                if (isSyn) {
                                    if (! containsNan(bandDataF, 6, sl01BandIndex, ndviBandIndex, i)) {
                                        final int stateCount = count(state, bandDataS, i);
                                        accu[1][i] += stateCount;
                                        for (int b = 5; b < numTargetBands; ++b) {
                                            final int bs = b + 1;
                                            if (bs < sl01BandIndex || bs > sl01BandIndex + 4) {
                                                accu[b][i] += stateCount * bandDataF[bs][i];  // we may have processed under clouds
                                            }
                                        }
                                    }
                                    if (! containsNan(bandDataF, sl01BandIndex, sl01BandIndex+3, ndviBandIndex, i)) {
                                        final int stateCount = count(state, bandDataS, i);
                                        accu[3][i] += stateCount;
                                        for (int bs = sl01BandIndex; bs < sl01BandIndex + 3; ++bs) {
                                            final int b = bs - 1;
                                            accu[b][i] += stateCount * bandDataF[bs][i];
                                        }
                                    }
                                    if (! containsNan(bandDataF, sl01BandIndex+3, sl01BandIndex+5, ndviBandIndex, i)) {
                                        final int stateCount = count(state, bandDataS, i);
                                        accu[4][i] += stateCount;
                                        for (int bs = sl01BandIndex + 3; bs < sl01BandIndex + 5; ++bs) {
                                            final int b = bs - 1;
                                            accu[b][i] += stateCount * bandDataF[bs][i];
                                        }
                                    }
                                } else {
                                    if (! containsNan(bandDataF, numTargetBands, ndviBandIndex, i)) {
                                        final int stateCount = count(state, bandDataS, i);
                                        accu[1][i] += stateCount;
                                        for (int b = 3; b < numTargetBands; ++b) {
                                            accu[b][i] += stateCount * bandDataF[b + 3][i];  // we may have processed under clouds
                                        }
                                    }
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
                if (state > 0 && ! containsNan(bandDataF, numTargetBands, ndviBandIndex, i)) {
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
                                       int ndviBandIndex, int numSourceBands, int numTargetBands,
                                       float[][] accu) {
        for (MultiLevelImage[] bandImage : bandImages) {
            readSourceBands(bandImage, microTileArea, numSourceBands, bandDataB, bandDataS, bandDataF);
            // pixel loop
            for (int i = 0; i < microTileArea.height * microTileArea.width; ++i) {
                final int state = (int) bandDataB[0][i];
                if (state > 0 && ! containsNan(bandDataF, numTargetBands, ndviBandIndex, i)) {
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

    private void divideByCount(Rectangle microTileArea, int numTargetBands, boolean isSyn, int sl01BandIndex, float[][] accu) {
        for (int i = 0; i < microTileArea.height * microTileArea.width; ++i) {
            if (isSyn) {
                final float stateCount1 = accu[1][i];
                for (int b = 5; b < numTargetBands; ++b) {
                    final int bs = b + 1;
                    if (bs < sl01BandIndex || bs > sl01BandIndex + 4) {
                        if (stateCount1 > 0) {
                            accu[b][i] /= stateCount1;
                        } else {
                            accu[b][i] = Float.NaN;
                        }
                    }
                }
                final float stateCount2 = accu[3][i];
                for (int bs = sl01BandIndex; bs < sl01BandIndex + 3; ++bs) {
                    final int b = bs - 1;
                    if (stateCount2 > 0) {
                        accu[b][i] /= stateCount2;
                    } else {
                        accu[b][i] = Float.NaN;
                    }
                }
                final float stateCount3 = accu[4][i];
                for (int bs = sl01BandIndex + 3; bs < sl01BandIndex + 5; ++bs) {
                    final int b = bs - 1;
                    if (stateCount3 > 0) {
                        accu[b][i] /= stateCount3;
                    } else {
                        accu[b][i] = Float.NaN;
                    }
                }
            } else {
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
    }


    private void clearAccu(int numTargetBands, float[][] accu) {
        for (int b = 0; b < numTargetBands; b++) {
            Arrays.fill(accu[b], 0.0f);
        }
    }

    private void clearNdxi(int numBandGroups) {
        for (int j=0; j<numBandGroups * NUM_INDEXES; ++j) {
            Arrays.fill(ndxiSum[j], 0.0f);
            Arrays.fill(ndxiSqrSum[j], 0.0f);
            Arrays.fill(ndxiCount[j], 0);
        }
        for (int j=0; j<NUM_INDEXES; ++j) {
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

    private void readNdviNdwiBands(MultiLevelImage[] bandImage, int b3BandIndex, int b11BandIndex, int ndviBandIndex, boolean isSyn, int sl01BandIndex, Rectangle microTileArea, float[][] bandDataF) {
        bandDataF[b3BandIndex] = (float[]) ImageUtils.getPrimitiveArray(bandImage[b3BandIndex].getData(microTileArea).getDataBuffer());
        bandDataF[b11BandIndex] = (float[]) ImageUtils.getPrimitiveArray(bandImage[b11BandIndex].getData(microTileArea).getDataBuffer());
        bandDataF[ndviBandIndex] = (float[]) ImageUtils.getPrimitiveArray(bandImage[ndviBandIndex].getData(microTileArea).getDataBuffer());
        if (isSyn) {
            bandDataF[sl01BandIndex + 1] = (float[]) ImageUtils.getPrimitiveArray(bandImage[sl01BandIndex + 1].getData(microTileArea).getDataBuffer());
            bandDataF[sl01BandIndex + 2] = (float[]) ImageUtils.getPrimitiveArray(bandImage[sl01BandIndex + 2].getData(microTileArea).getDataBuffer());
            bandDataF[sl01BandIndex + 3] = (float[]) ImageUtils.getPrimitiveArray(bandImage[sl01BandIndex + 3].getData(microTileArea).getDataBuffer());
            bandDataF[sl01BandIndex + 4] = (float[]) ImageUtils.getPrimitiveArray(bandImage[sl01BandIndex + 4].getData(microTileArea).getDataBuffer());
        }
    }

    private boolean containsNan(float[][] bandDataF, int numTargetBands, int ndviBandIndex, int i) {
        for (int b = 3; b < numTargetBands; ++b) {
            if (b+3 == ndviBandIndex ? Float.isNaN(bandDataF[b + 3][i]) : isNaN(bandDataF[b + 3][i])) {
                return true;
            }
        }
        return false;
    }

    private boolean containsNan(float[][] bandDataF, int startIndex, int stopIndex, int ndviBandIndex, int i) {
        for (int bs = startIndex; bs < stopIndex; ++bs) {
            if (bs == ndviBandIndex ? Float.isNaN(bandDataF[bs][i]) : isNaN(bandDataF[bs][i])) {
                return true;
            }
        }
        return false;
    }

    private static boolean within1Sigma(float value, float mean, float sdev) {
        return value >= mean - sdev - EPS && value <= mean + sdev + EPS;
    }

    private static float ndxiOf(float nir, float red) {
        return (nir - red) / (nir + red);
    }

    private static float sdevOf(float sqrSum, float mean, int count) {
        return (float) Math.sqrt(sqrSum / count - mean * mean);
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

    private Product sdrToSrSyn(Product product) {
        Map<String, Object> parameters = new HashMap<>();
        BandMathsOp.BandDescriptor[] bandDescriptors = new BandMathsOp.BandDescriptor[27];

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
        bandDescriptors[6].name = "sr_oa01_mean";
        bandDescriptors[6].expression = "sdr_Oa01";
        bandDescriptors[6].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[7] = new BandMathsOp.BandDescriptor();
        bandDescriptors[7].name = "sr_oa02_mean";
        bandDescriptors[7].expression = "sdr_Oa02";
        bandDescriptors[7].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[8] = new BandMathsOp.BandDescriptor();
        bandDescriptors[8].name = "sr_oa03_mean";
        bandDescriptors[8].expression = "sdr_Oa03";
        bandDescriptors[8].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[9] = new BandMathsOp.BandDescriptor();
        bandDescriptors[9].name = "sr_oa04_mean";
        bandDescriptors[9].expression = "sdr_Oa04";
        bandDescriptors[9].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[10] = new BandMathsOp.BandDescriptor();
        bandDescriptors[10].name = "sr_oa05_mean";
        bandDescriptors[10].expression = "sdr_Oa05";
        bandDescriptors[10].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[11] = new BandMathsOp.BandDescriptor();
        bandDescriptors[11].name = "sr_oa06_mean";
        bandDescriptors[11].expression = "sdr_Oa06";
        bandDescriptors[11].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[12] = new BandMathsOp.BandDescriptor();
        bandDescriptors[12].name = "sr_oa07_mean";
        bandDescriptors[12].expression = "sdr_Oa07";
        bandDescriptors[12].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[13] = new BandMathsOp.BandDescriptor();
        bandDescriptors[13].name = "sr_oa08_mean";
        bandDescriptors[13].expression = "sdr_Oa08";
        bandDescriptors[13].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[14] = new BandMathsOp.BandDescriptor();
        bandDescriptors[14].name = "sr_oa09_mean";
        bandDescriptors[14].expression = "sdr_Oa09";
        bandDescriptors[14].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[15] = new BandMathsOp.BandDescriptor();
        bandDescriptors[15].name = "sr_oa10_mean";
        bandDescriptors[15].expression = "sdr_Oa10";
        bandDescriptors[15].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[16] = new BandMathsOp.BandDescriptor();
        bandDescriptors[16].name = "sr_oa12_mean";
        bandDescriptors[16].expression = "sdr_Oa12";
        bandDescriptors[16].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[17] = new BandMathsOp.BandDescriptor();
        bandDescriptors[17].name = "sr_oa16_mean";
        bandDescriptors[17].expression = "sdr_Oa16";
        bandDescriptors[17].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[18] = new BandMathsOp.BandDescriptor();
        bandDescriptors[18].name = "sr_oa17_mean";
        bandDescriptors[18].expression = "sdr_Oa17";
        bandDescriptors[18].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[19] = new BandMathsOp.BandDescriptor();
        bandDescriptors[19].name = "sr_oa18_mean";
        bandDescriptors[19].expression = "sdr_Oa18";
        bandDescriptors[19].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[20] = new BandMathsOp.BandDescriptor();
        bandDescriptors[20].name = "sr_oa21_mean";
        bandDescriptors[20].expression = "sdr_Oa21";
        bandDescriptors[20].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[21] = new BandMathsOp.BandDescriptor();
        bandDescriptors[21].name = "sr_sl01_mean";
        bandDescriptors[21].expression = "sdr_Sl01";
        bandDescriptors[21].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[22] = new BandMathsOp.BandDescriptor();
        bandDescriptors[22].name = "sr_sl02_mean";
        bandDescriptors[22].expression = "sdr_Sl02";
        bandDescriptors[22].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[23] = new BandMathsOp.BandDescriptor();
        bandDescriptors[23].name = "sr_sl03_mean";
        bandDescriptors[23].expression = "sdr_Sl03";
        bandDescriptors[23].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[24] = new BandMathsOp.BandDescriptor();
        bandDescriptors[24].name = "sr_sl05_mean";
        bandDescriptors[24].expression = "sdr_Sl05";
        bandDescriptors[24].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[25] = new BandMathsOp.BandDescriptor();
        bandDescriptors[25].name = "sr_sl06_mean";
        bandDescriptors[25].expression = "sdr_Sl06";
        bandDescriptors[25].type = ProductData.TYPESTRING_FLOAT32;

        bandDescriptors[26] = new BandMathsOp.BandDescriptor();
        bandDescriptors[26].name = "vegetation_index_mean";
        bandDescriptors[26].expression = "ndvi_max";
        bandDescriptors[26].type = ProductData.TYPESTRING_FLOAT32;

        parameters.put("targetBands", bandDescriptors);
        product = GPF.createProduct("BandMaths", parameters, product);
        return product;
    }

    private int majorityPriorityStatusOf(int[][] statusCount, int i) {
        return (statusCount[1][i] > 0 && statusCount[1][i] >= statusCount[0][i] && statusCount[1][i] >= statusCount[2][i]) ? 2 :  // more water than land or snow
               (statusCount[0][i] > 0 && statusCount[0][i] >= statusCount[2][i]) ? 1 :  // more land than snow
               statusCount[2][i] > 0 ? 3 :   // some snow
               statusCount[3][i] > 0 ? 15 :  // dark
               statusCount[4][i] > 0 ? 12 :  // bright
               statusCount[5][i] > 0 ? 11 :  // haze
               statusCount[6][i] > 0 ? 5 :   // shadow
               statusCount[7][i] > 0 ? 4 :   // cloud
                       0;                    // invalid
    }

    static int sourceBandIndexOf(String sensorAndResolution, int targetBandIndex) {
        return
            "MERIS-300m".equals(sensorAndResolution) ? 2 * targetBandIndex :  // sr_1 is source 6 target 3 etc.
            "MERIS-1000m".equals(sensorAndResolution) ? 2 * targetBandIndex :  // sr_1 is source 6 target 3 etc.
            "OLCI-L3".equals(sensorAndResolution) ? targetBandIndex + 3 :  // sr_1 is source 6 target 3 etc.
            "SYN-L3".equals(sensorAndResolution) ? targetBandIndex + 1 :  // sr_1 is source 6 target 5 etc.
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
            case "SYN-L3":
                return SYN_BANDS;
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