package com.bc.calvalus.processing.l3.seasonal;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.beam.TemporalAggregator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.util.ImageUtils;
import org.esa.snap.core.util.ProductUtils;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.bc.calvalus.processing.l3.seasonal.SeasonalCompositingReducer.MERIS_BANDS;
import static com.bc.calvalus.processing.l3.seasonal.SeasonalCompositingReducer.SYN_BANDS;

/**
 * Temporal aggregator for priority status determination in the course of seasonal compositing.
 * The step also determines ndvi mean and standard deviation for later application of a 1-sigma filter.
 * Inputs are usually SR products with status, 5 counts, the sr bands, and an ndvi band.
 * For OLCI and SYN the inputs are L3 of SDR.
 *
 * @author Martin Boettcher
 */
public class SeasonalBestPixelsAggregator implements TemporalAggregator {
    // SYN-L3-P1D-h20v08-20210101-S3B-1.12.2.nc
    private static Pattern SR_FILENAME_PATTERN =
            Pattern.compile("(?:ESACCI-LC-L3-SR-|)([^-]*-[^-]*)-[^-]*-h([0-9]*)v([0-9]*)-........(-[^-]*)?-(.*).nc");
    private static final SimpleDateFormat DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat COMPACT_DATE_FORMAT = DateUtils.createDateFormat("yyyyMMdd");
    protected static final Logger LOG = CalvalusLogger.getLogger();
    static int NUM_INDEXES = 8;
    static final int STATUS_CLOUD_SHADOW = 5;
    static final float EPS = 1.0E-6f;

    static String[] SYN_BAND_NAMES = {
            "current_pixel_state",
            "num_obs",
            "sdr_Oa01",
            "sdr_Oa02",
            "sdr_Oa03",
            "sdr_Oa04",
            "sdr_Oa05",
            "sdr_Oa06",
            "sdr_Oa07",
            "sdr_Oa08",
            "sdr_Oa09",
            "sdr_Oa10",
            "sdr_Oa12",
            "sdr_Oa16",
            "sdr_Oa17",
            "sdr_Oa18",
            "sdr_Oa21",
            "sdr_Sl01",
            "sdr_Sl02",
            "sdr_Sl03",
            "sdr_Sl05",
            "sdr_Sl06",
            "ndvi_max"
    };

    static String[] OLCI_BAND_NAMES = {
            "current_pixel_state",
            "ndvi_max",
            "sdr_Oa06_mean",
            "sdr_Oa18_mean_list_incomplete"
    };
    public static String[] MSI_BAND_NAMES = {
            "current_pixel_state",
            "vegetation_index_mean",
            "B3_mean",
            "B11_mean_list_incomplete"
    };

    String statusBandName;
    String ndviBandName;
    String greenBandName;
    String swirBandName;
    String sl02BandName;
    String sl03BandName;
    String sl05BandName;
    String sl06BandName;

    boolean isMsi;
    boolean isOlci;
    boolean isSyn;

    private float srThreshold;

    int sceneRasterHeight;
    int sceneRasterWidth;

    float[][] ndviMean;
    float[][] ndviSdev;
    float[][] sl32Mean;
    float[][] sl32Sdev;
    float[][] sl65Mean;
    float[][] sl65Sdev;

    int tileColumn;
    int tileRow;
    Date startDate;
    Date stopDate;
    Product bestPixelProduct;

    int numSourceBands;
    int numTargetBands;
    int b3BandIndex;
    int b11BandIndex;
    int ndviBandIndex;
    int sl01BandIndex;
    String[] sourceBands;
    String[] targetBands;
    float[][] accu;

    public void initialize(Configuration conf, Product firstInput) {
        sceneRasterHeight = firstInput.getSceneRasterHeight();
        sceneRasterWidth = firstInput.getSceneRasterWidth();

        final File someTilePath = firstInput.getFileLocation();
        final Matcher matcher = SR_FILENAME_PATTERN.matcher(someTilePath.getName());
        if (! matcher.matches())  {
           throw new IllegalArgumentException(someTilePath.getName() + " does not match " + SR_FILENAME_PATTERN);
        }
        final String sensorAndResolution = matcher.group(1);
        tileColumn = Integer.parseInt(matcher.group(2), 10);
        tileRow = Integer.parseInt(matcher.group(3), 10);
        isMsi = sensorAndResolution.startsWith("MSI");
        isOlci = sensorAndResolution.startsWith("OLCI");
        isSyn = sensorAndResolution.startsWith("SYN");

        // read configuration
        startDate = getDate(conf, JobConfigNames.CALVALUS_MIN_DATE);
        stopDate = getDate(conf, JobConfigNames.CALVALUS_MAX_DATE);
        try {
            srThreshold = Float.parseFloat(System.getProperty("calvalus.compositing.srthreshold", "NaN"));
            LOG.info("using sr threshold " + srThreshold + " (numbers lower than that are considered NaN, switched off if NaN itself");
            LOG.info("testing isNaN: " + isNaN(-0.6f) + " " + isNaN(-0.4f) + " " + isNaN(Float.NaN));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cannot parse value of calvalus.compositing.srthreshold '" +
                                                       System.getProperty("calvalus.compositing.srthreshold") + "' as a number: " + e);
        }

        // determine target bands and indexes
        sourceBands = isSyn ? SYN_BAND_NAMES : isOlci ? OLCI_BAND_NAMES : isMsi ? MSI_BAND_NAMES : null;
        targetBands = sensorBandsOf(sensorAndResolution);
        numTargetBands = targetBands.length;
        numSourceBands = sourceBands.length;
        // for SYN the OLCI NDWI is computed from Oa17 and Oa08
        b3BandIndex = isMsi ? 6 - 1 + 2 : isOlci ? 6-1+5 : isSyn ? 2+7 : 6 - 1 + 4;    // green 560 nm
        b11BandIndex = isMsi ? 6 - 1 + 9 : isOlci ? 6-1+13 : isSyn ? 2+12: 6 - 1 + 3;  // swir-1 1610 nm
        ndviBandIndex = numSourceBands - 1;
        sl01BandIndex = isSyn ? 2+15 : -1;

        accu = new float[numTargetBands][sceneRasterHeight * sceneRasterWidth];

        statusBandName = sourceBands[0];
        ndviBandName = sourceBands[sourceBands.length-1];
        greenBandName = sourceBands[isSyn ? 2+7 : isOlci ? 5+5 : isMsi ? 5+2 : 5+4];
        swirBandName = sourceBands[isSyn ? 2+12 : isOlci ? 5+13 : isMsi ? 5+9 : 5+3];
        sl02BandName = isSyn ? sourceBands[2+15+1] : null;
        sl03BandName = isSyn ? sourceBands[2+15+2] : null;
        sl05BandName = isSyn ? sourceBands[2+15+3] : null;
        sl06BandName = isSyn ? sourceBands[2+15+4] : null;

        try {
            readStatus(conf);
        } catch (IOException e) {
            throw new RuntimeException("failed to read 'primajority' status from " + conf.get("calvalus.l2.parameters"), e);
        }

        bestPixelProduct =
                new Product(String.format("BestPixel-h%02dv%02d-%s", tileColumn, tileRow, COMPACT_DATE_FORMAT.format(startDate)),
                            "BestPixelTile",
                            sceneRasterWidth,
                            sceneRasterHeight);
        ProductUtils.copyGeoCoding(firstInput, bestPixelProduct);
        ProductUtils.copyMetadata(firstInput, bestPixelProduct);
    }

    private void readStatus(Configuration conf) throws IOException {
        final String processorParameters = conf.get("calvalus.l2.parameters");
        final String statusUrl = processorParameters.split(",")[0].split("=")[1];
        final Path statusPath = new Path(statusUrl);
        File inputFile = CalvalusProductIO.copyFileToLocal(statusPath, conf);
        LOG.info("reading primajority status from " + inputFile.getName());
        ndviMean = new float[sceneRasterHeight][sceneRasterWidth];
        ndviSdev = new float[sceneRasterHeight][sceneRasterWidth];
        if (isSyn) {
            sl32Mean = new float[sceneRasterHeight][sceneRasterWidth];
            sl32Sdev = new float[sceneRasterHeight][sceneRasterWidth];
            sl65Mean = new float[sceneRasterHeight][sceneRasterWidth];
            sl65Sdev = new float[sceneRasterHeight][sceneRasterWidth];
        } else {
            sl32Mean = null;
            sl32Sdev = null;
            sl65Mean = null;
            sl65Sdev = null;
        }
        Product product = ProductIO.readProduct(inputFile);
        Band statusBand = product.getBand("status");
        Band ndviMeanBand = product.getBand("ndviMean");
        Band ndviSdevBand = product.getBand("ndviSdev");
        statusBand.readRasterDataFully();
        ndviMeanBand.readRasterDataFully();
        ndviSdevBand.readRasterDataFully();
        Band sl32MeanBand;
        Band sl32SdevBand;
        Band sl65MeanBand;
        Band sl65SdevBand;
        if (isSyn) {
            sl32MeanBand = product.getBand("sl32Mean");
            sl32SdevBand = product.getBand("sl32Sdev");
            sl65MeanBand = product.getBand("sl65Mean");
            sl65SdevBand = product.getBand("sl65Sdev");
            sl32MeanBand.readRasterDataFully();
            sl32SdevBand.readRasterDataFully();
            sl65MeanBand.readRasterDataFully();
            sl65SdevBand.readRasterDataFully();
        } else {
            sl32MeanBand = null;
            sl32SdevBand = null;
            sl65MeanBand = null;
            sl65SdevBand = null;
        }
        for (int row = 0; row < sceneRasterHeight; ++row) {
            for (int col = 0; col < sceneRasterWidth; ++col) {
                final int i = row * sceneRasterWidth + col;
                // status
                accu[0][i] = statusBand.getSampleInt(col, row);
                ndviMean[row][col] = ndviMeanBand.getSampleFloat(col, row);
                ndviSdev[row][col] = ndviSdevBand.getSampleFloat(col, row);
                if (isSyn) {
                    sl32Mean[row][col] = sl32MeanBand.getSampleFloat(col, row);
                    sl32Sdev[row][col] = sl32SdevBand.getSampleFloat(col, row);
                    sl65Mean[row][col] = sl65MeanBand.getSampleFloat(col, row);
                    sl65Sdev[row][col] = sl65SdevBand.getSampleFloat(col, row);
                }
            }
        }
    }

    /** aggregate is called once per input. The first input is provided to init and to aggregate. */
    public void aggregate(Product contribution) throws IOException {
        final Band[] sourceBands = new Band[numSourceBands];
        final float[][] bandDataB = new float[numSourceBands][];
        final int[][] bandDataS = new int[numSourceBands][];
        final float[][] bandDataF = new float[numSourceBands][];
        for (int b = 0; b < numSourceBands; ++b) {
            sourceBands[b] = contribution.getBand(this.sourceBands[b]);
            //sourceBands[b].readRasterDataFully();
            if (b < 1) {
                bandDataB[b] = (float[]) ImageUtils.getPrimitiveArray(sourceBands[b].getSourceImage().getData().getDataBuffer());
            } else if (b < (isSyn ? 2 : 6)) {
                bandDataS[b] = (int[]) ImageUtils.getPrimitiveArray(sourceBands[b].getSourceImage().getData().getDataBuffer());
            } else {
                bandDataF[b] = (float[]) ImageUtils.getPrimitiveArray(sourceBands[b].getSourceImage().getData().getDataBuffer());
            }
        }
        for (int row = 0; row < sceneRasterHeight; ++row) {
            for (int col = 0; col < sceneRasterWidth; ++col) {
                final int i = row * sceneRasterWidth + col;
                final byte state = (byte) bandDataB[0][i];
                if (state > 0) {
                    // obs_count
                    accu[2][i] += isSyn ? bandDataS[1][i] : count(bandDataS, i);
                    if (state == accu[0][i]) {
                        switch (state) {
                            case 1:
                            case 15:
                            case 12:
                            case 11:
                            case 5:
                                if (isSyn) {
                                    final int stateCount = 1;
                                    if (! containsNan(bandDataF, 2, sl01BandIndex, ndviBandIndex, i)) {
                                        float ndvi = bandDataF[ndviBandIndex][i];
                                        if (within1Sigma(ndvi, ndviMean[row][col], ndviSdev[row][col])) {
                                            // status_count
                                            accu[1][i] += stateCount;
                                            for (int b = 5; b < numTargetBands; ++b) {
                                                final int bs = b - 3;  // sr_1 is b=5 and bs=2
                                                if (bs < sl01BandIndex || bs > sl01BandIndex + 4) {
                                                    accu[b][i] += stateCount * bandDataF[bs][i];
                                                }
                                            }
                                        }
                                    }
                                    if (! containsNan(bandDataF, sl01BandIndex, sl01BandIndex+3, ndviBandIndex, i)) {
                                        float ndxi123 = ndxiOf(bandDataF[sl01BandIndex + 2][i],
                                                               bandDataF[sl01BandIndex + 1][i]);
                                        if (within1Sigma(ndxi123, sl32Mean[row][col], sl32Sdev[row][col])) {
                                            // sl123_count
                                            accu[3][i] += stateCount;
                                            for (int bs = sl01BandIndex; bs < sl01BandIndex + 3; ++bs) {
                                                final int b = bs + 3;
                                                accu[b][i] += stateCount * bandDataF[bs][i];
                                            }
                                        }
                                    }
                                    if (! containsNan(bandDataF, sl01BandIndex+3, sl01BandIndex+5, ndviBandIndex, i)) {
                                        float ndxi56 = ndxiOf(bandDataF[sl01BandIndex + 4][i],
                                                              bandDataF[sl01BandIndex + 3][i]);
                                        if (within1Sigma(ndxi56, sl65Mean[row][col], sl65Sdev[row][col])) {
                                            // sl56_count
                                            accu[4][i] += stateCount;
                                            for (int bs = sl01BandIndex + 3; bs < sl01BandIndex + 5; ++bs) {
                                                final int b = bs + 3;
                                                accu[b][i] += stateCount * bandDataF[bs][i];
                                            }
                                        }
                                    }
                                } else {
                                    if (! containsNan(bandDataF, numTargetBands, ndviBandIndex, i)) {
                                        float ndvi = bandDataF[ndviBandIndex][i];
                                        if (within1Sigma(ndvi, ndviMean[row][col], ndviSdev[row][col])) {
                                            final int stateCount = count(state == 1 ? state : STATUS_CLOUD_SHADOW, bandDataS, i);  // cloud shadow count abused for dark, bright, haze
                                            accu[1][i] += stateCount;
                                            for (int b = 3; b < numTargetBands; ++b) {
                                                accu[b][i] += stateCount * bandDataF[b + 3][i];
                                            }
                                        }
                                    }
                                }
                                break;
                            case 2:
                            case 3:  // TODO TBC whether to use water index for snow as well
                                if (isSyn) {
                                    final int stateCount = 1;
                                    if (! containsNan(bandDataF, 2, sl01BandIndex, ndviBandIndex, i)) {
                                        float ndwi = ndxiOf(bandDataF[b11BandIndex][i],
                                                            bandDataF[b3BandIndex][i]);

                                        if (within1Sigma(ndwi, ndviMean[row][col], ndviSdev[row][col])) {
                                            // status_count
                                            accu[1][i] += stateCount;
                                            for (int b = 5; b < numTargetBands; ++b) {
                                                final int bs = b - 3;  // sr_1 is b=5 and bs=2
                                                if (bs < sl01BandIndex || bs > sl01BandIndex + 4) {
                                                    accu[b][i] += stateCount * bandDataF[bs][i];
                                                }
                                            }
                                        }
                                    }
                                    if (! containsNan(bandDataF, sl01BandIndex, sl01BandIndex+3, ndviBandIndex, i)) {
                                        float ndxi123 = ndxiOf(bandDataF[sl01BandIndex + 2][i],
                                                               bandDataF[sl01BandIndex + 1][i]);
                                        if (within1Sigma(ndxi123, sl32Mean[row][col], sl32Sdev[row][col])) {
                                            accu[3][i] += stateCount;
                                            for (int bs = sl01BandIndex; bs < sl01BandIndex + 3; ++bs) {
                                                final int b = bs + 3;
                                                accu[b][i] += stateCount * bandDataF[bs][i];
                                            }
                                        }
                                    }
                                    if (! containsNan(bandDataF, sl01BandIndex+3, sl01BandIndex+5, ndviBandIndex, i)) {
                                        float ndxi56 = ndxiOf(bandDataF[sl01BandIndex + 4][i],
                                                              bandDataF[sl01BandIndex + 3][i]);
                                        if (within1Sigma(ndxi56, sl65Mean[row][col], sl65Sdev[row][col])) {
                                            accu[4][i] += stateCount;
                                            for (int bs = sl01BandIndex + 3; bs < sl01BandIndex + 5; ++bs) {
                                                final int b = bs + 3;
                                                accu[b][i] += stateCount * bandDataF[bs][i];
                                            }
                                        }
                                    }
                                } else {
                                    if (! containsNan(bandDataF, numTargetBands, ndviBandIndex, i)) {
                                        float ndwi = ndxiOf(bandDataF[b11BandIndex][i],
                                                            bandDataF[b3BandIndex][i]);
                                        if (within1Sigma(ndwi, ndviMean[row][col], ndviSdev[row][col])) {
                                            final int stateCount = count(state, bandDataS, i);
                                            accu[1][i] += stateCount;
                                            for (int b = 3; b < numTargetBands; ++b) {
                                                accu[b][i] += stateCount * bandDataF[b + 3][i];
                                            }
                                        }
                                    }
                                }
                                break;
                            case 4:
                            case 14:
                                if (isSyn) {
                                    final int stateCount = 1;
                                    if (! containsNan(bandDataF, 2, sl01BandIndex, ndviBandIndex, i)) {
                                        accu[1][i] += stateCount;
                                        for (int b = 5; b < numTargetBands; ++b) {
                                            final int bs = b - 3;
                                            if (bs < sl01BandIndex || bs > sl01BandIndex + 4) {
                                                accu[b][i] += stateCount * bandDataF[bs][i];  // we may have processed under clouds
                                            }
                                        }
                                    }
                                    if (! containsNan(bandDataF, sl01BandIndex, sl01BandIndex+3, ndviBandIndex, i)) {
                                        accu[3][i] += stateCount;
                                        for (int bs = sl01BandIndex; bs < sl01BandIndex + 3; ++bs) {
                                            final int b = bs + 3;
                                            accu[b][i] += stateCount * bandDataF[bs][i];
                                        }
                                    }
                                    if (! containsNan(bandDataF, sl01BandIndex+3, sl01BandIndex+5, ndviBandIndex, i)) {
                                        accu[4][i] += stateCount;
                                        for (int bs = sl01BandIndex + 3; bs < sl01BandIndex + 5; ++bs) {
                                            final int b = bs + 3;
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
                                break;
                        }
                    }
                }
            }
        }
    }

    /** complete is called after the last input. It shall return the aggregated product. */
    public Product complete() {
        for (int row = 0; row < sceneRasterHeight; ++row) {
            for (int col = 0; col < sceneRasterWidth; ++col) {
                int i = row * sceneRasterWidth + col;
                if (isSyn) {
                    final int stateCount1 = (int)accu[1][i];
                    for (int b = 5; b < numTargetBands; ++b) {
                        final int bs = b - 3;
                        if (bs < sl01BandIndex || bs > sl01BandIndex + 4) {
                            if (stateCount1 > 0) {
                                accu[b][i] /= stateCount1;
                            } else {
                                accu[b][i] = Float.NaN;
                            }
                        }
                    }
                    final int stateCount2 = (int)accu[3][i];
                    for (int bs = sl01BandIndex; bs < sl01BandIndex + 3; ++bs) {
                        final int b = bs + 3;
                        if (stateCount2 > 0) {
                            accu[b][i] /= stateCount2;
                        } else {
                            accu[b][i] = Float.NaN;
                        }
                    }
                    final int stateCount3 = (int) accu[4][i];
                    for (int bs = sl01BandIndex + 3; bs < sl01BandIndex + 5; ++bs) {
                        final int b = bs + 3;
                        if (stateCount3 > 0) {
                            accu[b][i] /= stateCount3;
                        } else {
                            accu[b][i] = Float.NaN;
                        }
                    }
                } else {
                    final int stateCount = (int)accu[1][i];
                    for (int b = 3; b < numTargetBands; ++b) {
                        if (stateCount > 0) {
                            accu[b][i] /= stateCount;
                        } else {
                            accu[b][i] = Float.NaN;
                        }
                    }
                }
            }
        }

        for (int b = 0; b < 1; ++b) {
            final Band band = bestPixelProduct.addBand(targetBands[b], ProductData.TYPE_INT8);
            final byte[] data = new byte[accu[b].length];
            for (int i=0; i<sceneRasterHeight*sceneRasterWidth; ++i) {
                data[i] = (byte) accu[b][i];
            }
            band.setRasterData(new ProductData.Byte(data));
            band.getSourceImage();  // triggers that raster data will be used by writer
        }
        for (int b = 1; b < (isSyn ? 5 : 3); ++b) {
            final Band band = bestPixelProduct.addBand(targetBands[b], ProductData.TYPE_INT16);
            final short[] data = new short[accu[b].length];
            for (int i=0; i<sceneRasterHeight*sceneRasterWidth; ++i) {
                data[i] = (short) accu[b][i];
            }
            band.setRasterData(new ProductData.Short(data));
            band.getSourceImage();
        }
        for (int b = (isSyn ? 5 : 3); b < numTargetBands; ++b) {
            final Band band = bestPixelProduct.addBand(targetBands[b], ProductData.TYPE_FLOAT32);
            band.setRasterData(new ProductData.Float(accu[b]));
            band.getSourceImage();
        }

        return bestPixelProduct;
    }

    static int count(int[][] bandDataS, int i) {
        return bandDataS[1][i]
                + bandDataS[2][i]
                + bandDataS[3][i]
                + bandDataS[4][i]
                + bandDataS[5][i];
    }

    static int count(int state, int[][] bandDataS, int i) {
        switch (state) {
            case 1: return bandDataS[1][i];
            case 2: return bandDataS[2][i];
            case 3: return bandDataS[3][i];
            case 4: return bandDataS[4][i];
            case 5:
            case 12:
            case 11:
            case 15: return bandDataS[5][i];
            default: return 0;
        }
    }

    private boolean containsNan(float[][] bandDataF, int numTargetBands, int ndviBandIndex, int i) {
        for (int b = 3; b < numTargetBands; ++b) {
            if (b+3 == ndviBandIndex ?
                    Float.isNaN(bandDataF[b + 3][i]) :
                    isNaN(bandDataF[b + 3][i])) {
                return true;
            }
        }
        return false;
    }

    private boolean containsNan(float[][] bandDataF, int startIndex, int stopIndex, int ndviBandIndex, int i) {
        for (int bs = startIndex; bs < stopIndex; ++bs) {
            if (bs == ndviBandIndex ?
                    Float.isNaN(bandDataF[bs][i]) :
                    isNaN(bandDataF[bs][i])) {
                return true;
            }
        }
        return false;
    }

    private static boolean within1Sigma(float value, float mean, float sdev) {
        // return value >= mean - sdev - EPS && value <= mean + sdev + EPS;
        return ! (value < mean - sdev - EPS || value > mean + sdev + EPS);
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

    boolean isNaN(float f) {
        return Float.isNaN(f) || f < srThreshold;
    }

    static Date getDate(Configuration conf, String parameterName) {
        try {
            return DATE_FORMAT.parse(conf.get(parameterName));
        } catch (ParseException e) {
            throw new IllegalArgumentException("parameter " + parameterName + " value " + conf.get(parameterName) +
                                               " does not match pattern " + DATE_FORMAT.toPattern() + ": " + e.getMessage(), e);
        }
    }

    private static float ndxiOf(float nir, float red) {
        return (nir - red) / (nir + red);
    }
}
