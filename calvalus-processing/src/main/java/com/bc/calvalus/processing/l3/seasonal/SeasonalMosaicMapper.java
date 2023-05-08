package com.bc.calvalus.processing.l3.seasonal;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
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
import org.esa.snap.core.datamodel.Band;
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
public class SeasonalMosaicMapper extends Mapper<NullWritable, NullWritable, IntWritable, BandTileWritable> {

    protected static final Logger LOG = CalvalusLogger.getLogger();

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

    private static final Pattern TILE_FILENAME_PATTERN =
            Pattern.compile("([^-]*)-(....-..-..)-(P.*)-h([0-9]*)v([0-9]*)-(.*).nc");

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        GpfUtils.init(context.getConfiguration());
        Engine.start();  // required here!  we do not use a ProcessorAdapter
        CalvalusLogger.restoreCalvalusLogFormatter();
        
        // /calvalus/projects/c3s/syn-seasonal-tiles-v1.12.11/2021-01-01-P52W/h20v09/SYN-2021-01-01-P52W-h20v09-tile-v1.12.11.nc
        final Path tilePath = ((FileSplit) context.getInputSplit()).getPath();
        final File tileFile = CalvalusProductIO.copyFileToLocal(tilePath, context.getConfiguration());
        final Product tileProduct = ProductIO.readProduct(tileFile);

        final Matcher matcher = TILE_FILENAME_PATTERN.matcher(tilePath.getName());
        if (! matcher.matches()) {
            throw new IllegalArgumentException("file name " + tilePath.getName() + " does not match pattern " + TILE_FILENAME_PATTERN.pattern());
        }
        String sensorAndResolution = matcher.group(1);
        if ("SYN".equals(sensorAndResolution)) {
            sensorAndResolution = "SYN-L3";
        }
        final int tileColumn = Integer.parseInt(matcher.group(4), 10);
        final int tileRow = Integer.parseInt(matcher.group(5), 10);
        final boolean isMsi = sensorAndResolution.startsWith("MSI");
        final boolean isOlci = sensorAndResolution.startsWith("OLCI");
        final boolean isSyn = sensorAndResolution.startsWith("SYN");

        // read configuration
        final Configuration conf = context.getConfiguration();
        final int mosaicHeight;
        try {
            final BinningConfig binningConfig = BinningConfig.fromXml(conf.get(JobConfigNames.CALVALUS_L3_PARAMETERS));
            mosaicHeight = binningConfig.getNumRows();
        } catch (BindingException e) {
            throw new IllegalArgumentException("L3 parameters not well formed: " + e.getMessage() + " in " + conf.get(JobConfigNames.CALVALUS_L3_PARAMETERS));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cannot parse value of calvalus.compositing.srthreshold '" +
                                               System.getProperty("calvalus.compositing.srthreshold") + "' as a number: " + e);
        }

        // sensor-dependent resolution parameters
        final int numTileRows = isMsi ? 180 : isOlci ? 18 : isSyn ? 18 : 36;
        final int numMicroTiles = isMsi ? 5 : isOlci ? 2 : isSyn ? 2 : 1;
        final int tileSize = mosaicHeight / numTileRows;
        final int microTileSize = tileSize / numMicroTiles;

        // determine target bands and indexes
        String[] sensorBands = sensorBandsOf(sensorAndResolution);

        // micro tile loop
        LOG.info("processing " + (numMicroTiles*numMicroTiles) + " micro tiles ...");
        for (int microTileY = 0; microTileY < numMicroTiles; ++microTileY) {
            for (int microTileX = 0; microTileX < numMicroTiles; ++microTileX) {
                final Rectangle microTileArea = new Rectangle(microTileX * microTileSize, microTileY * microTileSize, microTileSize, microTileSize);

                // stream results, one per band
                for (int b = 0; b < sensorBands.length; ++b) {
                    final float[] data;
                    if (b == 0) {
                        data = getByteArray(tileProduct.getBand(sensorBands[b]), microTileArea);
                    } else if (b < 5) {
                        data = getShortArray(tileProduct.getBand(sensorBands[b]), microTileArea);
                    } else {
                        data = getFloatArray(tileProduct.getBand(sensorBands[b]), microTileArea);
                    }
                    // compose key from band and tile  TODO b instead of targetBandIndex???
                    final int bandAndTile = ((sensorBands.length - (isSyn ? 1 : 3)) << 26) + (b << 21) + ((tileRow * numMicroTiles + microTileY) << 11) + (tileColumn * numMicroTiles + microTileX);
                    // write tile
                    final IntWritable key = new IntWritable(bandAndTile);
                    final BandTileWritable value = new BandTileWritable(data);
                    context.write(key, value);
                }
            }
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

    private float[] getFloatArray(Band band, Rectangle area) {
        return (float[]) ImageUtils.getPrimitiveArray(band.getSourceImage().getData(area).getDataBuffer());
    }
    private float[] getShortArray(Band band, Rectangle area) {
        short[] b = (short[])ImageUtils.getPrimitiveArray(band.getSourceImage().getData(area).getDataBuffer());
        float[] f = new float[b.length];
        for (int i=0; i<b.length; ++i) {
            f[i] = (short) b[i];
        }
        return f;
    }
    private float[] getByteArray(Band band, Rectangle area) {
        byte[] b = (byte[])ImageUtils.getPrimitiveArray(band.getSourceImage().getData(area).getDataBuffer());
        float[] f = new float[b.length];
        for (int i=0; i<b.length; ++i) {
            f[i] = (float) b[i];
        }
        return f;
    }
}