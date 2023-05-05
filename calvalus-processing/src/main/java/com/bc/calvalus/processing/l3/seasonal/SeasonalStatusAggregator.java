package com.bc.calvalus.processing.l3.seasonal;

import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.TemporalAggregator;
import org.apache.hadoop.conf.Configuration;
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

/**
 * Temporal aggregator for priority status determination in the course of seasonal compositing.
 * The step also determines ndvi mean and standard deviation for later application of a 1-sigma filter.
 * Inputs are usually SR products with status, 5 counts, the sr bands, and an ndvi band.
 * For OLCI and SYN the inputs are L3 of SDR.
 *
 * @author Martin Boettcher
 */
public class SeasonalStatusAggregator implements TemporalAggregator {
    // SYN-L3-P1D-h20v08-20210101-S3B-1.12.2.nc
    private static Pattern SR_FILENAME_PATTERN =
            Pattern.compile("(?:ESACCI-LC-L3-SR-|)([^-]*-[^-]*)-[^-]*-h([0-9]*)v([0-9]*)-........(-[^-]*)?-(.*).nc");
    private static final SimpleDateFormat DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat COMPACT_DATE_FORMAT = DateUtils.createDateFormat("yyyyMMdd");
    protected static final Logger LOG = CalvalusLogger.getLogger();
    static int NUM_INDEXES = 8;

    static String[] SYN_BAND_NAMES = {
            "current_pixel_state",
            "ndvi_max",
            "sdr_Oa18",
            "sdr_Oa06",
            "sdr_Sl02",
            "sdr_Sl03",
            "sdr_Sl05",
            "sdr_Sl06"
    };

    static String[] OLCI_BAND_NAMES = {
            "current_pixel_state",
            "ndvi_max",
            "sdr_Oa18_mean",
            "sdr_Oa06_mean"
    };
    public static String[] MSI_BAND_NAMES = {
            "current_pixel_state",
            "vegetation_index_mean",
            "B3_mean",
            "B11_mean"
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

    float[][][] ndxiSum;
    float[][][] ndxiSqrSum;
    int[][][] ndxiCount;
    int[][][] statusCount;

    int sceneRasterHeight;
    int sceneRasterWidth;

    int tileColumn;
    int tileRow;
    Date startDate;
    Date stopDate;
    Product primajorityStatusProduct;

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

        final String[] bandNames = isSyn ? SYN_BAND_NAMES : isOlci ? OLCI_BAND_NAMES : isMsi ? MSI_BAND_NAMES : null;
        statusBandName = bandNames[0];
        ndviBandName = bandNames[1];
        greenBandName = bandNames[2];  // NDWI for SYN is (Oa06 - Oa18) / (Oa06 + Oa18), i.e. Oa18 is "green"
        swirBandName = bandNames[3];
        sl02BandName = isSyn ? bandNames[4] : null;
        sl03BandName = isSyn ? bandNames[5] : null;
        sl05BandName = isSyn ? bandNames[6] : null;
        sl06BandName = isSyn ? bandNames[7] : null;

        // pre-allocate arrays for band values per data type, for best pixels aggregation
        statusCount = new int[NUM_INDEXES][sceneRasterHeight][sceneRasterWidth];
        ndxiSum = new float[(isSyn ? 3 : 1) * NUM_INDEXES][sceneRasterHeight][sceneRasterWidth];
        ndxiSqrSum = new float[(isSyn ? 3 : 1) * NUM_INDEXES][sceneRasterHeight][sceneRasterWidth];
        ndxiCount = new int[(isSyn ? 3 : 1) * NUM_INDEXES][sceneRasterHeight][sceneRasterWidth];

        primajorityStatusProduct =
                new Product(String.format("PrimajorityStatus-h%02dv%02d-%s", tileColumn, tileRow, COMPACT_DATE_FORMAT.format(startDate)),
                            "PrimajorityStatus",
                            sceneRasterWidth,
                            sceneRasterHeight);
        ProductUtils.copyGeoCoding(firstInput, primajorityStatusProduct);
        ProductUtils.copyMetadata(firstInput, primajorityStatusProduct);
    }

    /** aggregate is called once per input. The first input is provided to init and to aggregate. */
    public void aggregate(Product contribution) throws IOException {
        final float[] statusData = getDataArray(contribution.getBand(statusBandName));
        final float[] ndviData = getDataArray(contribution.getBand(ndviBandName));
        final float[] greenData = getDataArray(contribution.getBand(greenBandName));
        final float[] swirData = getDataArray(contribution.getBand(swirBandName));
        final float[] sl02Data;
        final float[] sl03Data;
        final float[] sl05Data;
        final float[] sl06Data;
        if (isSyn) {
            sl02Data = getDataArray(contribution.getBand(sl02BandName));
            sl03Data = getDataArray(contribution.getBand(sl03BandName));
            sl05Data = getDataArray(contribution.getBand(sl05BandName));
            sl06Data = getDataArray(contribution.getBand(sl06BandName));
        } else {
            sl02Data = null;
            sl03Data = null;
            sl05Data = null;
            sl06Data = null;
        }
        for (int row = 0; row < sceneRasterHeight; ++row) {
            for (int col = 0; col < sceneRasterWidth; ++col) {
                final int i = row * sceneRasterWidth + col;
                // count status
                final byte state = (byte) statusData[i];
                final int index = index(state);
                if (index >= 0) {
                    statusCount[index][row][col]++;
                }
                if (index >= 0) {
                    switch (state) {
                        // collect land ndvi for mean and stddev
                        case 1:
                        case 15:
                        case 12:
                        case 11:
                        case 5:
                            final float ndvi = ndviData[i];
                            aggregateNdxi(row, col, index, ndvi);
                            if (isSyn) {
                                final float ndxi32 = ndxiOf(sl03Data[i], sl02Data[i]);
                                aggregateNdxi(row, col, NUM_INDEXES + index, ndxi32);
                                final float ndxi65 = ndxiOf(sl06Data[i], sl05Data[i]);
                                aggregateNdxi(row, col, NUM_INDEXES * 2 + index, ndxi65);
                            }
                            break;
                        // collect water (and snow) ndwi for mean and stddev
                        case 2:
                        case 3:  // TODO TBC whether to use water index for snow as well
                            float ndwi = ndxiOf(swirData[i], greenData[i]);
                            aggregateNdxi(row, col, index, ndwi);
                            // we use the same normalised measures as for land as we do not have proper NDWI in one of the SLSTR groups
                            if (isSyn) {
                                final float ndxi32 = ndxiOf(sl03Data[i], sl02Data[i]);
                                aggregateNdxi(row, col, NUM_INDEXES + index, ndxi32);
                                final float ndxi65 = ndxiOf(sl06Data[i], sl05Data[i]);
                                aggregateNdxi(row, col, NUM_INDEXES * 2 + index, ndxi65);
                            }
                            break;
                    }
                }
            }
        }
    }

    /** complete is called after the last input. It shall return the aggregated product. */
    public Product complete() {
        // calculate mean and stddev from collected sums, squares, and counts
        final byte[] status = new byte[sceneRasterHeight * sceneRasterWidth];
        final float[] ndviMean = new float[sceneRasterHeight * sceneRasterWidth];
        final float[] ndviSdev = new float[sceneRasterHeight * sceneRasterWidth];
        final float[] sl32Mean;
        final float[] sl32Sdev;
        final float[] sl65Mean;
        final float[] sl65Sdev;
        if (isSyn) {
            sl32Mean = new float[sceneRasterHeight * sceneRasterWidth];
            sl32Sdev = new float[sceneRasterHeight * sceneRasterWidth];
            sl65Mean = new float[sceneRasterHeight * sceneRasterWidth];
            sl65Sdev = new float[sceneRasterHeight * sceneRasterWidth];
        } else {
            sl32Mean = null;
            sl32Sdev = null;
            sl65Mean = null;
            sl65Sdev = null;
        }
        for (int row = 0; row < sceneRasterHeight; ++row) {
            for (int col = 0; col < sceneRasterWidth; ++col) {
                int i = row * sceneRasterWidth + col;
                byte state = majorityPriorityStatusOf(statusCount, row, col);
                int index = index(state);
                if (index >= 0) {
                    status[i] = state;
                }
                if (index >= 0 && index < 7) {
                    if (ndxiCount[index][row][col] > 0) {
                        ndviMean[i] = ndxiSum[index][row][col] / ndxiCount[index][row][col];
                        ndviSdev[i] = sdevOf(ndxiSqrSum[index][row][col], ndviMean[i], ndxiCount[index][row][col]);
                    } else {
                        ndviMean[i] = Float.NaN;
                        ndviSdev[i] = Float.NaN;
                    }
                    if (isSyn) {
                        if (ndxiCount[NUM_INDEXES + index][row][col] > 0) {
                            sl32Mean[i] = ndxiSum[NUM_INDEXES + index][row][col] / ndxiCount[NUM_INDEXES + index][row][col];
                            sl32Sdev[i] = sdevOf(ndxiSqrSum[NUM_INDEXES + index][row][col], sl32Mean[i], ndxiCount[NUM_INDEXES + index][row][col]);
                        } else {
                            sl32Mean[i] = Float.NaN;
                            sl32Sdev[i] = Float.NaN;
                        }
                        if (ndxiCount[2 * NUM_INDEXES + index][row][col] > 0) {
                            sl65Mean[i] = ndxiSum[2 * NUM_INDEXES + index][row][col] / ndxiCount[2 * NUM_INDEXES + index][row][col];
                            sl65Sdev[i] = sdevOf(ndxiSqrSum[2 * NUM_INDEXES + index][row][col], sl65Mean[i], ndxiCount[2 * NUM_INDEXES + index][row][col]);
                        } else {
                            sl65Mean[i] = Float.NaN;
                            sl65Sdev[i] = Float.NaN;
                        }
                    }
                } else {  // invalid or cloud or temporal cloud
                    ndviMean[i] = Float.NaN;
                    ndviSdev[i] = Float.NaN;
                    if (isSyn) {
                        sl32Mean[i] = Float.NaN;
                        sl32Sdev[i] = Float.NaN;
                        sl65Mean[i] = Float.NaN;
                        sl65Sdev[i] = Float.NaN;
                    }
                }
            }
        }
        // create output product with status, mean and stddev of ndvi
        final Band statusBand = primajorityStatusProduct.addBand("status", ProductData.TYPE_INT8);
        statusBand.setRasterData(new ProductData.Byte(status));
        statusBand.getSourceImage();   // needed to make raster data be used by writer
        final Band ndviMeanBand = primajorityStatusProduct.addBand("ndviMean", ProductData.TYPE_FLOAT32);
        ndviMeanBand.setRasterData(new ProductData.Float(ndviMean));
        ndviMeanBand.getSourceImage();
        final Band ndviSdevBand = primajorityStatusProduct.addBand("ndviSdev", ProductData.TYPE_FLOAT32);
        ndviSdevBand.setRasterData(new ProductData.Float(ndviSdev));
        ndviSdevBand.getSourceImage();
        if (isSyn) {
            final Band sl32MeanBand = primajorityStatusProduct.addBand("sl32Mean", ProductData.TYPE_FLOAT32);
            sl32MeanBand.setRasterData(new ProductData.Float(sl32Mean));
            sl32MeanBand.getSourceImage();
            final Band sl32SdevBand = primajorityStatusProduct.addBand("sl32Sdev", ProductData.TYPE_FLOAT32);
            sl32SdevBand.setRasterData(new ProductData.Float(sl32Sdev));
            sl32SdevBand.getSourceImage();
            final Band sl65MeanBand = primajorityStatusProduct.addBand("sl65Mean", ProductData.TYPE_FLOAT32);
            sl65MeanBand.setRasterData(new ProductData.Float(sl65Mean));
            sl65MeanBand.getSourceImage();
            final Band sl65SdevBand = primajorityStatusProduct.addBand("sl65Sdev", ProductData.TYPE_FLOAT32);
            sl65SdevBand.setRasterData(new ProductData.Float(sl65Sdev));
            sl65SdevBand.getSourceImage();
        }
        return primajorityStatusProduct;
    }

    static int index(byte state) {
        switch (state) {
            case 1:
                return 0;
            case 2:
                return 1;
            case 3:
                return 2;
            case 15:
                return 3;  // dark
            case 12:
                return 4;  // bright
            case 11:
                return 5;  // haze
            case 5:
                return 6;  // shadow
            case 4:
            case 14:
                return 7;  // cloud or temporal cloud
            default:
                return -1;
        }
    }

    private byte majorityPriorityStatusOf(int[][][] statusCount, int row, int col) {
        return (statusCount[1][row][col] > 0 && statusCount[1][row][col] >= statusCount[0][row][col] && statusCount[1][row][col] >= statusCount[2][row][col]) ? (byte)2 :  // more water than land or snow
               (statusCount[0][row][col] > 0 && statusCount[0][row][col] >= statusCount[2][row][col]) ? (byte)1 :  // more land than snow
               statusCount[2][row][col] > 0 ? (byte)3 :   // some snow
               statusCount[3][row][col] > 0 ? (byte)15 :  // dark
               statusCount[4][row][col] > 0 ? (byte)12 :  // bright
               statusCount[5][row][col] > 0 ? (byte)11 :  // haze
               statusCount[6][row][col] > 0 ? (byte)5 :   // shadow
               statusCount[7][row][col] > 0 ? (byte)4 :   // cloud
                       (byte)0;                    // invalid
    }

    private static float ndxiOf(float nir, float red) {
        return (nir - red) / (nir + red);
    }

    private static float sdevOf(float sqrSum, float mean, int count) {
        return count > 1 ? (float) Math.sqrt(sqrSum / (count - 1) - mean * mean) : 0.0f;
    }

    private void aggregateNdxi(int row, int col, int index, float ndvi) {
        if (!Float.isNaN(ndvi)) {
            ndxiSum[index][row][col] += ndvi;
            ndxiSqrSum[index][row][col] += ndvi * ndvi;
            ndxiCount[index][row][col]++;
        }
    }

    private float[] getDataArray(Band band) {
        return (float[]) ImageUtils.getPrimitiveArray(band.getSourceImage().getData().getDataBuffer());
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
}
