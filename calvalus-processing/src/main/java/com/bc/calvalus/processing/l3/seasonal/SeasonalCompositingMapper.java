package com.bc.calvalus.processing.l3.seasonal;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.glevel.MultiLevelImage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.binning.operator.BinningConfig;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.util.ImageUtils;

import java.awt.Point;
import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class SeasonalCompositingMapper extends Mapper<NullWritable, NullWritable, IntWritable, BandTileWritable> {

    protected static final Logger LOG = CalvalusLogger.getLogger();

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat COMPACT_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
    private static final SimpleDateFormat YEAR_FORMAT = new SimpleDateFormat("yyyy");
    private static final String SR_FILENAME_FORMAT = "ESACCI-LC-L3-SR-%s-P7D-h%02dv%02d-%s-%s.nc";
    private static final Pattern SR_FILENAME_PATTERN =
            Pattern.compile("ESACCI-LC-L3-SR-([^-]*-[^-]*)-[^-]*-h([0-9][0-9])v([0-9][0-9])-........-([^-]*).nc");
    public static final int NUM_SRC_BANDS = 1 + 5 + 13 + 1;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        // determine path of some input, weeks
        // /calvalus/eodata/MERIS_SR_FR/v1.0/2010/2010-01-01/ESACCI-LC-L3-SR-MERIS-300m-P7D-h36v08-20100101-v1.0.nc
        final Configuration conf = context.getConfiguration();
        final int mosaicHeight;
        try {
            mosaicHeight = BinningConfig.fromXml(conf.get(JobConfigNames.CALVALUS_L3_PARAMETERS)).getNumRows();
        } catch (BindingException e) {
            throw new IllegalArgumentException("no numRows in L3 parameters " + conf.get(JobConfigNames.CALVALUS_L3_PARAMETERS));
        }
        final int bandTileHeight = mosaicHeight / 36;  // 64800 / 36 = 1800, 16200 / 36 = 450
        final int bandTileWidth = bandTileHeight;
        final Path someTilePath = ((FileSplit) context.getInputSplit()).getPath();
        final Path srRootDir = someTilePath.getParent().getParent().getParent();
        final FileSystem fs = someTilePath.getFileSystem(conf);

        final Matcher matcher = SR_FILENAME_PATTERN.matcher(someTilePath.getName());
        if (! matcher.matches()) {
            throw new IllegalArgumentException("file name " + someTilePath.getName() + " does not match pattern " + SR_FILENAME_PATTERN.pattern());
        }
        final String sensorAndResolution = matcher.group(1);
        final int tileColumn = Integer.parseInt(matcher.group(2), 10);
        final int tileRow = Integer.parseInt(matcher.group(3), 10);
        final String version = matcher.group(4);

        final Date start = getDate(conf, JobConfigNames.CALVALUS_MIN_DATE);
        final Date stop = getDate(conf, JobConfigNames.CALVALUS_MAX_DATE);

        // initialise aggregation variables array, status, statusCount, count, bands 1-10,12-14, ndvi
        float[][] accu = null;
        int numInputBands = 0;
        int numTargetBands = 0;
        // loop over weeks
        for (Date week = start; ! stop.before(week); week = nextWeek(week)) {

            // determine and read input tile for the week
            final String weekFileName = String.format(SR_FILENAME_FORMAT, sensorAndResolution, tileColumn, tileRow, COMPACT_DATE_FORMAT.format(week), version);
            final Path path = new Path(new Path(new Path(srRootDir, YEAR_FORMAT.format(week)), DATE_FORMAT.format(week)), weekFileName);
            if (! fs.exists(path)) {
                LOG.info("skipping non-existing week " + path);
                continue;
            }
            final Product product = readProduct(conf, fs, path);
            if (accu == null) {
                numInputBands = product.getBands().length;
                // 0 is currentPixelState, 1..5 are the count bands,
                // 6,8,..30 is sr_1_mean..10,12,13,14, or
                // 6,8,10,11,12 are the AVHRR bands or
                // 6 .. n the are the SPOT bands
                // 32 is vegetation_index_mean, or 11 is vegetation_index_mean
                if (numInputBands == 1 + 5 + 2 * 13 + 1) {
                    numTargetBands = 13 + 1;
                } else if (numInputBands == 1 + 5 + 2*2 + 3 + 1) {
                    numTargetBands = 5 + 1;
                } else {
                    numTargetBands = numInputBands - 6;
                }
                accu = new float[numTargetBands + 3][bandTileHeight * bandTileWidth];
            }
            final MultiLevelImage[] bandImage = new MultiLevelImage[numTargetBands+6];
            for (int b = 0; b < numTargetBands+6; ++b) {
                if (b < 6 || numTargetBands == numInputBands-6) {
                    bandImage[b] = product.getBandAt(b).getGeophysicalImage();
                } else if (numTargetBands == 5 + 1) {
                    bandImage[b] = product.getBandAt(b==6?6:b==7?8:b+2).getGeophysicalImage();
                } else {
                    // bandAt(6) is sr_1_mean, bandAt(7) is sr_1_uncertainty, ...
                    bandImage[b] = product.getBandAt(2*b-6).getGeophysicalImage();
                }
            }

            LOG.info("aggregating week " + weekFileName);

            // image tile loop
            for (Point tileIndex : bandImage[0].getTileIndices(null)) {
                final Raster[] bandTile = new Raster[NUM_SRC_BANDS];
                final short[][] bandDataB = new short[NUM_SRC_BANDS][];
                final short[][] bandDataS = new short[NUM_SRC_BANDS][];
                final float[][] bandDataF = new float[NUM_SRC_BANDS][];
                for (int b = 0; b < numTargetBands+6; b++) {
                    bandTile[b] = bandImage[b].getTile(tileIndex.x, tileIndex.y);
                    if (b == 0) {
                        bandDataB[b] = (short[]) ImageUtils.getPrimitiveArray(bandTile[b].getDataBuffer());
                    } else if (b < 6) {
                        bandDataS[b] = (short[]) ImageUtils.getPrimitiveArray(bandTile[b].getDataBuffer());
                    } else {
                        bandDataF[b] = (float[]) ImageUtils.getPrimitiveArray(bandTile[b].getDataBuffer());
                    }
                }
                final int tileMinX = bandTile[0].getMinX();
                final int tileMinY = bandTile[0].getMinY();
                final int tileWidth = bandTile[0].getWidth();
                final int tileHeight = bandTile[0].getHeight();

                // pixel loop
                for (int r = 0; r < tileHeight; ++r) {
                    for (int c = 0; c < tileWidth; ++c) {
                        final int iSrc = r * tileWidth + c;
                        final int iDst = (tileMinY + r) * bandTileWidth + tileMinX + c;

                        // aggregate pixel-wise using aggregation rules
                        final int state = (int) bandDataB[0][iSrc];
                        if (state <= 0) {
                            continue;
                        }
                        if (state == accu[0][iDst]) {
                            // same state as before, aggregate ...
                            final int stateCount = count(state, bandDataS, iSrc);
                            accu[1][iDst] += stateCount;
                            accu[2][iDst] += count(bandDataS, iSrc);
                            for (int b = 3; b < numTargetBands+3; ++b) {
                                accu[b][iDst] += stateCount * bandDataF[b+3][iSrc];
                            }
                        } else if (rank(state) > rank(accu[0][iDst])) {
                            // better state, e.g. land instead of snow: restart counting ...
                            final int stateCount = count(state, bandDataS, iSrc);
                            accu[0][iDst] = state;
                            accu[1][iDst] = stateCount;
                            accu[2][iDst] = count(bandDataS, iSrc);
                            for (int b = 3; b < numTargetBands+3; ++b) {
                                accu[b][iDst] = stateCount * bandDataF[b+3][iSrc];
                            }
                        }
                    }
                }
                //LOG.info("image tile y " + tileIndex.y + " x " + tileIndex.x);
            }

            product.dispose();
        }

        // finish aggregation, divide by stateCount
        for (int r = 0; r < bandTileHeight; ++r) {
            for (int c = 0; c < bandTileWidth; ++c) {
                final int i = r * bandTileWidth + c;
                final float stateCount = accu[1][i];
                //if (stateCount > 0) {
                    for (int b = 3; b < numTargetBands+3; ++b) {
                        accu[b][i] /= stateCount;
                    }
                //}
            }
        }

        // statistics for logging
        final int[] counts = new int[6];
        for (float state : accu[0]) {
            ++counts[rank(state)];
        }
        LOG.info(counts[5] + " land, " + counts[4] + " water, " + counts[3] + " snow, " + counts[2] + " shadow, " + counts[1] + " cloud");

        // stream results, one per band
        for (int b = 0; b < numTargetBands+3; ++b) {
            // compose key from band and tile
            final int bandAndTile = (numTargetBands << 24) + (b << 16) + (tileRow << 8) + tileColumn;
            LOG.info("streaming band " + b + " tile row " + tileRow + " tile column " + tileColumn + " key " + bandAndTile);
            // write tile
            final IntWritable key = new IntWritable(bandAndTile);
            final BandTileWritable value = new BandTileWritable(accu[b]);
            context.write(key, value);
        }
    }

    private static Product readProduct(Configuration conf, FileSystem fs, Path path) throws IOException {
        final File localFile = new File(path.getName());
        FileUtil.copy(fs, path, localFile, false, conf);
        return ProductIO.readProduct(localFile);
    }

    private static Date getDate(Configuration conf, String parameterName) {
        try {
            return DATE_FORMAT.parse(conf.get(parameterName));
        } catch (ParseException e) {
            throw new IllegalArgumentException("parameter " + parameterName + " value " + conf.get(parameterName) +
                                               " does not match pattern " + DATE_FORMAT.toPattern() + ": " + e.getMessage(), e);
        }
    }

    static Date nextWeek(Date week) {
        GregorianCalendar c = new GregorianCalendar();
        c.setTime(week);
        c.add(Calendar.DATE, lengthOfWeek(c));
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

    private static int count(short[][] bandData, int iSrc) {
        return (int) bandData[1][iSrc]
                + (int) bandData[2][iSrc]
                + (int) bandData[3][iSrc]
                + (int) bandData[4][iSrc]
                + (int) bandData[5][iSrc];
    }

    private static int count(int state, short[][] bandData, int iSrc) {
        if (state == 1) {
            return (int) bandData[1][iSrc];
        } else if (state == 2) {
            return (int) bandData[2][iSrc];
        } else if (state == 3) {
            return (int) bandData[3][iSrc];
/* UCL does not count cloud and cloud shadow
        } else if (state == 4) {
            return (int) bandData[4][iSrc];
*/
        } else if (state == 5) {
            return (int) bandData[5][iSrc];
        } else {
            return 0;
        }
    }

    private static int rank(float state) {
        switch ((int) state) {
            case 1: return 5;
            case 2: return 4;
            case 3: return 3;
            case 5: return 2;
            case 4: return 1;
//            case 4: return 2;
//            case 5: return 1;
            default: return 0;
        }
    }
}