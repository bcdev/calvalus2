package com.bc.calvalus.processing.l3.seasonal;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.GpfUtils;
import com.bc.calvalus.processing.l2.ProductFormatter;
import com.bc.calvalus.processing.l3.HadoopBinManager;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.core.ProgressMonitor;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.binning.PlanetaryGrid;
import org.esa.snap.binning.Reprojector;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.runtime.Engine;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.Rectangle;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

/**
 * TBD
 */
public class SeasonalCompositingReducer extends Reducer<IntWritable, BandTileWritable, NullWritable, NullWritable> {

    static {
        System.getProperties().put("snap.dataio.bigtiff.compression.type", "LZW");
        System.getProperties().put("snap.dataio.bigtiff.tiling.width", "256");
        System.getProperties().put("snap.dataio.bigtiff.tiling.height", "256");
        System.getProperties().put("snap.dataio.bigtiff.force.bigtiff", "true");
    }

    protected static final Logger LOG = CalvalusLogger.getLogger();

    private static final SimpleDateFormat DATE_FORMAT = DateUtils.createDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat COMPACT_DATE_FORMAT = DateUtils.createDateFormat("yyyyMMdd");

    public static String[] MERIS_BANDS = {
            "status",
            "status_count",
            "obs_count",
            "sr_1_mean",
            "sr_2_mean",
            "sr_3_mean",
            "sr_4_mean",
            "sr_5_mean",
            "sr_6_mean",
            "sr_7_mean",
            "sr_8_mean",
            "sr_9_mean",
            "sr_10_mean",
            "sr_12_mean",
            "sr_13_mean",
            "sr_14_mean",
            "vegetation_index_mean"
    };
    public static String[] AVHRR_BANDS = {
            "status",
            "status_count",
            "obs_count",
            "sr_1_mean",
            "sr_2_mean",
            "bt_3_mean",
            "bt_4_mean",
            "bt_5_mean",
            "vegetation_index_mean"
    };
    public static String[] PROBA_BANDS = {
            "status",
            "status_count",
            "obs_count",
            "sr_1_mean",
            "sr_2_mean",
            "sr_3_mean",
            "sr_4_mean",
            "vegetation_index_mean"
    };
    public static String[] MSI_BANDS = {
            "status",
            "status_count",
            "obs_count",
            "sr_1_mean",
            "sr_2_mean",
            "sr_3_mean",
            "sr_4_mean",
            "sr_5_mean",
            "sr_6_mean",
            "sr_7_mean",
            "sr_8_mean",
            "sr_9_mean",
            "sr_10_mean",
            "sr_11_mean",
            "vegetation_index_mean"
    };
    public static String[] AGRI_BANDS = {
            "status",
            "status_count",
            "obs_count",
            "sr_1_mean",
            "sr_2_mean",
            "sr_3_mean",
            "sr_4_mean",
            "sr_5_mean",
            "sr_6_mean",
            "sr_7_mean",
            "sr_8_mean",
            "sr_9_mean",
            "sr_10_mean",
            "vegetation_index_mean"
    };

    private final List<float[]> usedTiles = new ArrayList<>();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();

        if (! context.nextKey()) {
            LOG.info("no tiles found");
            return;
        }

        GpfUtils.init(context.getConfiguration());
        Engine.start();  // required here!  we do not use a ProcessorAdapter
        CalvalusLogger.restoreCalvalusLogFormatter();

        final int bandNumber = (context.getCurrentKey().get() >>> 22) & 0x1f;
        final int numberOfBands = context.getCurrentKey().get() >>> 27;
        final String sensor =
                "OLCI".equals(conf.get("calvalus.lc.sensor", "unknown")) ? "OLCI" :
                        numberOfBands == 13 + 1 ? "MERIS" :
                                numberOfBands == 5 + 1 ? "AVHRR" :
                                        numberOfBands == 4 + 1 ? "PROBAV" :
                                                numberOfBands == 4 + 1 ? "VEGETATION" :
                                                        numberOfBands == 11 + 1 ? "MSI" : "AGRI";
        final String bandName = "MERIS".equals(sensor) || "OLCI".equals(sensor) ? MERIS_BANDS[bandNumber] :
                "AVHRR".equals(sensor) ? AVHRR_BANDS[bandNumber] :
                        "VEGETATION".equals(sensor) ? PROBA_BANDS[bandNumber] :
                                "PROBAV".equals(sensor) ? PROBA_BANDS[bandNumber] :
                                        "MSI".equals(sensor) ? MSI_BANDS[bandNumber] : AGRI_BANDS[bandNumber];

        final Date start = getDate(conf, JobConfigNames.CALVALUS_MIN_DATE);
        final Date stop = getDate(conf, JobConfigNames.CALVALUS_MAX_DATE);
        final int noOfDays = (int) ((stop.getTime() - start.getTime()) / 86400 / 1000 + 1);

        LOG.info("reducing " + sensor + " band " + bandName + " " + noOfDays + " days starting at " + DATE_FORMAT.format(start));

        final int mosaicHeight;
        try {
            mosaicHeight = BinningConfig.fromXml(conf.get(JobConfigNames.CALVALUS_L3_PARAMETERS)).getNumRows();
        } catch (BindingException e) {
            throw new IllegalArgumentException("no numRows in L3 parameters " + conf.get(JobConfigNames.CALVALUS_L3_PARAMETERS));
        }
        final int resolution = mosaicHeight == 972000 ? 20 : mosaicHeight == 64800 ? 300 : mosaicHeight == 16200 ? 1000 : mosaicHeight == 20160 ? 1000 : mosaicHeight == 60480 ? 333 : 19440000 / mosaicHeight;
        final int numTileRows = "MSI".equals(sensor) || "AGRI".equals(sensor) ? 180*5 : "OLCI".equals(sensor) ? 18*2 : 36;
        final int numTileColumns = 2 * numTileRows;
        final int tileSize = mosaicHeight / numTileRows;  // 64800 / 36 = 1800, 16200 / 36 = 450, 972000 / 72*5 = 2700

        final String regionWkt = conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY, null);
        final String regionName;
        final Rectangle pixelArea;
        final Rectangle tileArea;
        if (regionWkt == null) {
            regionName = null;
            pixelArea = new Rectangle(0, 0, numTileColumns * tileSize, numTileRows * tileSize);
            tileArea = new Rectangle(0, 0, numTileColumns, numTileRows);
        } else {
            regionName = conf.get(JobConfigNames.CALVALUS_INPUT_REGION_NAME, "regional");
            final Geometry regionGeometry = GeometryUtils.createGeometry(regionWkt);
            final BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
            final PlanetaryGrid planetaryGrid = binningConfig.createPlanetaryGrid();
            tileArea = tileAreaOf(Reprojector.computeRasterSubRegion(planetaryGrid, regionGeometry), tileSize);
            pixelArea = pixelAreaOf(tileArea, tileSize);
            LOG.info("tile area=" + tileArea);
        }

        final String version = conf.get(JobConfigNames.CALVALUS_LC_VERSION, "2.0");
        final double pixelRef = conf.getDouble("calvalus.lc.pixelref", "PROBAV".equals(sensor) ? 0.5 : 0.0);
        final String targetFileName = String.format("ESACCI-LC-L3-SR-%s-%dm-P%d%s-%s-%s%s-v%s",
                                                    sensor, resolution,
                                                    "MSI".equals(sensor) || "AGRI".equals(sensor) ? noOfDays : noOfDays / 7,
                                                    "MSI".equals(sensor) || "AGRI".equals(sensor) ? "D" : "W",
                                                    bandName,
                                                    regionName == null ? "" : regionName + "-",
                                                    COMPACT_DATE_FORMAT.format(start), version);
        final String outputDirName = conf.get("calvalus.output.dir");
        final Product dimapOutput = new Product(targetFileName, bandName + " of seasonal composite", pixelArea.width, pixelArea.height);
        try {
            final double pixelSize = 180.0D / mosaicHeight;
            final CrsGeoCoding outputGeoCoding =
                    new CrsGeoCoding(DefaultGeographicCRS.WGS84,
                                     pixelArea.width, pixelArea.height,
                                     -180.0D + pixelSize * (double) pixelArea.x, 90.0D - pixelSize * (double) pixelArea.y,
                                     pixelSize, pixelSize,
                                     pixelRef, pixelRef);
            dimapOutput.setSceneGeoCoding(outputGeoCoding);
        } catch (FactoryException | TransformException e) {
            throw new IllegalArgumentException("failed to create CRS geocoding: " + e.getMessage(), e);
        }

        final Band band;
        float[] floatBuffer = null;
        short[] shortBuffer = null;
        byte[] byteBuffer = null;
        ProductData dimapData;
        if (bandNumber == 0) {
            band = dimapOutput.addBand(bandName, ProductData.TYPE_INT8);
            byteBuffer = new byte[pixelArea.width];
            dimapData = ProductData.createInstance(byteBuffer);
        } else if (bandNumber < 3) {
            band = dimapOutput.addBand(bandName, ProductData.TYPE_INT16);
            shortBuffer = new short[pixelArea.width];
            dimapData = ProductData.createInstance(shortBuffer);
        } else {
            band = dimapOutput.addBand(bandName, ProductData.TYPE_FLOAT32);
            floatBuffer = new float[pixelArea.width];
            dimapData = ProductData.createInstance(floatBuffer);
        }
        // product metadata

        final String dimapFileName = targetFileName + ".dim";
        LOG.info("writing to " + targetFileName + ".dim");
        final ProductWriter dimapWriter = ProductIO.getProductWriter(ProductIO.DEFAULT_FORMAT_NAME);
        dimapWriter.writeProductNodes(dimapOutput, dimapFileName);

        boolean moreTilesAvailable = true;
        final float[][] tiles = new float[numTileColumns][];
        // loop over micro tile rows, e.g. up to 36 for OLCI
        for (int tileRow = (context.getCurrentKey().get() >>> 11) & 0x7FF; tileRow < numTileRows && moreTilesAvailable; ++tileRow) {

            //LOG.info("processing tile row " + tileRow);
            // sort tiles of tile row into tile columns
            int count = 0;
            while (moreTilesAvailable && ((context.getCurrentKey().get() >>> 11) & 0x7FF) == tileRow) {
                final int tileColumn = context.getCurrentKey().get() & 0x7FF;
                //LOG.info("looking at tile " + context.getCurrentKey().get() + " tile row " + tileRow + " column " + tileColumn);
                tiles[tileColumn] = copyOf(context.getValues().iterator().next().getTileData());
                ++count;
                if (! context.nextKey()) {
                    moreTilesAvailable = false;
                    LOG.info("no more tiles in context.");
                }
            }
            LOG.info(count + " micro tiles in tile row " + tileRow);

            if (tileRow >= tileArea.y && tileRow < tileArea.y + tileArea.height) {
                // write lines of tile row to output file
                LOG.info("writing rows " + tileRow + " to " + (tileRow+tileSize-1) + " to DIMAP intermediate");
                for (int r = 0; r < tileSize; r++) {
                    for (int tileColumn = tileArea.x; tileColumn < tileArea.x + tileArea.width; tileColumn++) {
                        if (tiles[tileColumn] != null) {
                            for (int c = 0; c < tileSize; c++) {
                                final int iSrc = r * tileSize + c;
                                final int iDst = (tileColumn - tileArea.x) * tileSize + c;
                                if (bandNumber == 0) {
                                    byteBuffer[iDst] = (byte) tiles[tileColumn][iSrc];
                                } else if (bandNumber < 3) {
                                    shortBuffer[iDst] = (short) tiles[tileColumn][iSrc];
                                } else {
                                    floatBuffer[iDst] = tiles[tileColumn][iSrc];
                                }
                            }
                        } else {
                            for (int c = 0; c < tileSize; c++) {
                                final int iDst = (tileColumn - tileArea.x) * tileSize + c;
                                if (bandNumber == 0) {
                                    byteBuffer[iDst] = 0;
                                } else if (bandNumber < 3) {
                                    shortBuffer[iDst] = 0;
                                } else {
                                    floatBuffer[iDst] = Float.NaN;
                                }
                            }
                        }
                    }
                    dimapWriter.writeBandRasterData(band,
                                                    0, (tileRow - tileArea.y) * tileSize + r,
                                                    tileArea.width * tileSize, 1,
                                                    dimapData, ProgressMonitor.NULL);
                }
            }

            // collect garbage
            for (int tileColumn = 0; tileColumn < numTileColumns; tileColumn++) {
                if (tiles[tileColumn] != null) {
                    usedTiles.add(tiles[tileColumn]);
                    tiles[tileColumn] = null;
                }
            }
        }

        dimapOutput.closeIO();

        LOG.info("converting dimap to geotiff ...");
        final Product dimapInput = ProductIO.readProduct(dimapFileName);
        final ProductWriter geotiffWriter = ProductIO.getProductWriter("GeoTIFF-BigTIFF");
        geotiffWriter.writeProductNodes(dimapInput, targetFileName + ".tif");
        geotiffWriter.writeBandRasterData(dimapInput.getBandAt(0), 0, 0, 0, 0, null, null);
        dimapInput.closeIO();
        geotiffWriter.close();

        final Path outputPath = new Path(outputDirName, targetFileName + ".tif");
        LOG.info("copying geotiff to " + outputPath);
        final OutputStream outputStream = outputPath.getFileSystem(conf).create(outputPath);
        final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outputStream));
        final InputStream in = new BufferedInputStream(new FileInputStream(new File(targetFileName + ".tif")));
        ProductFormatter.copyAndClose(in, out, context);

        LOG.info("copying done");
    }

    private Rectangle pixelAreaOf(Rectangle tileArea, int tileSize) {
        return new Rectangle(tileArea.x * tileSize,
                             tileArea.y * tileSize,
                             tileArea.width * tileSize,
                             tileArea.height * tileSize);
    }

    private Rectangle tileAreaOf(Rectangle pixelArea, int tileSize) {
        return new Rectangle(pixelArea.x / tileSize,
                             pixelArea.y / tileSize,
                             (pixelArea.x + pixelArea.width + tileSize - 1) / tileSize - pixelArea.x / tileSize,
                             (pixelArea.y + pixelArea.height + tileSize - 1) / tileSize - pixelArea.y / tileSize);
    }

    private static Date getDate(Configuration conf, String parameterName) {
        try {
            return DATE_FORMAT.parse(conf.get(parameterName));
        } catch (ParseException e) {
            throw new IllegalArgumentException("parameter " + parameterName + " value " + conf.get(parameterName) +
                                               " does not match pattern " + DATE_FORMAT.toPattern() + ": " + e.getMessage(), e);
        }
    }
    private float[] copyOf(float[] tileData) {
        if (usedTiles.isEmpty()) {
            usedTiles.add(new float[tileData.length]);
        }
        float[] copy = usedTiles.remove(0);
        System.arraycopy(tileData, 0, copy, 0, tileData.length);
        return copy;
    }
}
