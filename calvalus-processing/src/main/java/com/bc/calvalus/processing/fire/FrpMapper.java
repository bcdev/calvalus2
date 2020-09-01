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
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.hadoop.MetadataSerializer;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.calvalus.processing.l3.HadoopBinManager;
import com.bc.calvalus.processing.l3.L3SpatialBin;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.snap.binning.BinningContext;
import org.esa.snap.binning.DataPeriod;
import org.esa.snap.binning.SpatialBin;
import org.esa.snap.binning.SpatialBinConsumer;
import org.esa.snap.binning.SpatialBinner;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.operator.SpatialProductBinner;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.dataio.netcdf.util.NetcdfFileOpener;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";

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
    };

    enum GEODETIC_VARIABLES {
        latitude_in,
        longitude_in
    };

    private static final SimpleDateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static long THIRTY_YEARS;
    static {
        ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            THIRTY_YEARS = ISO_DATE_FORMAT.parse("2000-01-01T00:00:00.000Z").getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        String targetFormat = conf.get("targetFormat", "l2monthly");  // one of l2monthly, l3daily, l3cycle, l3monthly

        final Path inputPath = ((FileSplit) context.getInputSplit()).getPath();
        final File[] inputFiles = CalvalusProductIO.uncompressArchiveToCWD(inputPath, conf);
        int platformNumber = inputPath.getName().startsWith("S3A") ? 1 : inputPath.getName().startsWith("S3B") ? 2 : 0;

        final File frpFile = findByName("FRP_in.nc", inputFiles);
        final NetcdfFile frpNetcdf = openNetcdfFile(frpFile);
        final Array[] frpArrays = new Array[FRP_VARIABLES.values().length];
        for (FRP_VARIABLES v : FRP_VARIABLES.values()) {
            frpArrays[v.ordinal()] = frpNetcdf.findVariable(v.name()).read();
        }
        int numFires = frpNetcdf.findDimension("fires").getLength();

        int count = 0;
        Index index = frpArrays[FRP_VARIABLES.flags.ordinal()].getIndex();
        if ("l2monthly".equals(targetFormat)) {
            System.out.print("Time\tLatitude\tLongitude\tRow\tColumn\tFRP_MIR\tFRP_SWIR\tAREA\tday_flag\tf1_flag\tPlatform\tConfidence\n");
            for (int i=0; i<numFires; ++i) {
                // filter
                long time = frpArrays[FRP_VARIABLES.time.ordinal()].getLong(i);
                double latitude = frpArrays[FRP_VARIABLES.latitude.ordinal()].getDouble(i);
                double longitude = frpArrays[FRP_VARIABLES.longitude.ordinal()].getDouble(i);
                int row = frpArrays[FRP_VARIABLES.j.ordinal()].getInt(i);
                int col = frpArrays[FRP_VARIABLES.i.ordinal()].getShort(i);
                double frpMwir = frpArrays[FRP_VARIABLES.FRP_MWIR.ordinal()].getDouble(i);
                double frpSwir = frpArrays[FRP_VARIABLES.FRP_SWIR.ordinal()].getDouble(i);
                double area = frpArrays[FRP_VARIABLES.IFOV_area.ordinal()].getDouble(i);
                int flags = frpArrays[FRP_VARIABLES.flags.ordinal()].getInt(index.set(row, col));
                int used_channel = frpArrays[FRP_VARIABLES.used_channel.ordinal()].getInt(i);
                double confidence = frpArrays[FRP_VARIABLES.confidence.ordinal()].getDouble(i);
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
                L3SpatialBin bin = new L3SpatialBin(time, FRP_VARIABLES.values().length, 0);
                bin.getFeatureValues()[0] = (float) platformNumber;
                bin.getFeatureValues()[FRP_VARIABLES.latitude.ordinal()] = (float) latitude;
                bin.getFeatureValues()[FRP_VARIABLES.longitude.ordinal()] = (float) longitude;
                bin.getFeatureValues()[FRP_VARIABLES.j.ordinal()] = (float) row;
                bin.getFeatureValues()[FRP_VARIABLES.i.ordinal()] = (float) col;
                bin.getFeatureValues()[FRP_VARIABLES.FRP_MWIR.ordinal()] = (float) frpMwir;
                bin.getFeatureValues()[FRP_VARIABLES.FRP_SWIR.ordinal()] = (float) frpSwir;
                bin.getFeatureValues()[FRP_VARIABLES.IFOV_area.ordinal()] = (float) area;
                bin.getFeatureValues()[FRP_VARIABLES.flags.ordinal()] = (float) ((flags & 64) != 0 ? 1 : 0);
                bin.getFeatureValues()[FRP_VARIABLES.used_channel.ordinal()] = (float) used_channel;
                bin.getFeatureValues()[FRP_VARIABLES.confidence.ordinal()] = (float) confidence;
                context.write(new LongWritable(time), bin);
            }
            LOG.info(count + "/" + numFires + " records streamed of " + inputPath.getName());
            return;
        }

        final File geodeticFile = findByName("geodetic_in.nc", inputFiles);
        final NetcdfFile geodeticNetcdf = openNetcdfFile(geodeticFile);
        final Array[] geodeticArrays = new Array[GEODETIC_VARIABLES.values().length];
        for (GEODETIC_VARIABLES v : GEODETIC_VARIABLES.values()) {
            geodeticArrays[v.ordinal()] = geodeticNetcdf.findVariable(v.name()).read();
        }
        int columns = geodeticNetcdf.findDimension("columns").getLength();
        int rows = geodeticNetcdf.findDimension("rows").getLength();


        double aFloat = frpArrays[FRP_VARIABLES.FRP_MWIR.ordinal()].getDouble(99);

/*
        double F1_Fire_pixel_radiance(fires) ;
        double FRP_MWIR(fires) ;
        double FRP_SWIR(fires) ;
        double FRP_uncertainty_MWIR(fires) ;
        double FRP_uncertainty_SWIR(fires) ;
        double Glint_angle(fires) ;
        double IFOV_area(fires) ;
        double Radiance_window(fires) ;
        double S7_Fire_pixel_radiance(fires) ;
        double TCWV(fires) ;
        ubyte classification(fires) ;
        double confidence(fires) ;
        short i(fires) ;
        int j(fires) ;
        double latitude(fires) ;
        double longitude(fires) ;
        short n_SWIR_fire(fires) ;
        short n_cloud(fires) ;
        short n_water(fires) ;
        short n_window(fires) ;
        int64 time(fires) ;
        double transmittance_MWIR(fires) ;
        double transmittance_SWIR(fires) ;
        ubyte used_channel(fires) ;
*/
/*
            final Geometry regionGeometry = GeometryUtils.createGeometry(conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
            final BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
            final DataPeriod dataPeriod = HadoopBinManager.createDataPeriod(conf, binningConfig.getMinDataHour());
            final BinningContext binningContext = HadoopBinManager.createBinningContext(binningConfig, dataPeriod, regionGeometry);
            final SpatialBinEmitter spatialBinEmitter = new SpatialBinEmitter(context);
            final SpatialBinner spatialBinner = new SpatialBinner(binningContext, spatialBinEmitter);
*/
        Geometry regionGeometry = GeometryUtils.createGeometry(conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        final boolean generateEmptyAggregate = conf.getBoolean("calvalus.generateEmptyAggregate", false);
        BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);

        DataPeriod dataPeriod = HadoopBinManager.createDataPeriod(conf, binningConfig.getMinDataHour());

        BinningContext binningContext = HadoopBinManager.createBinningContext(binningConfig, dataPeriod, regionGeometry);
        final SpatialBinEmitter spatialBinEmitter = new SpatialBinEmitter(context);
        final SpatialBinner spatialBinner = new SpatialBinner(binningContext, spatialBinEmitter);
        final ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        LOG.info("processing input " + processorAdapter.getInputPath() + " ...");
        ProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        final int progressForProcessing = processorAdapter.supportsPullProcessing() ? 5 : 90;
        final int progressForBinning = processorAdapter.supportsPullProcessing() ? 90 : 20;
        pm.beginTask("Level 3", progressForProcessing + progressForBinning);
        try {
            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, progressForProcessing));
            if (product != null) {
                HashMap<Product, List<Band>> addedBands = new HashMap<>();
                long numObs;
                try {
                    numObs = SpatialProductBinner.processProduct(product,
                            spatialBinner,
                            addedBands,
                            SubProgressMonitor.create(pm, progressForBinning));
                } catch (IllegalArgumentException e) {
                    boolean isSmallProduct = product.getSceneRasterHeight() <= 2 || product.getSceneRasterWidth() <= 2;
                    boolean cannotConstructGeoCoding = isSmallProduct && e.getMessage().equals("The specified region, if not null, must intersect with the image`s bounds.");
                    if (cannotConstructGeoCoding) {
                        // ignore this product, but don't fail the process
                        numObs = 0;
                    } else {
                        // something else is wrong that must be handled elsewhere.
                        throw e;
                    }
                }
                if (numObs > 0L) {
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product with pixels").increment(1);
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Pixel processed").increment(numObs);

                } else {
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product without pixels").increment(1);
                }
                if (numObs > 0 || generateEmptyAggregate) {
                    final String metaXml = extractProcessingGraphXml(product);
                    context.write(new LongWritable(L3SpatialBin.METADATA_MAGIC_NUMBER), new L3SpatialBin(metaXml));
                }

            } else {
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product not used").increment(1);
                LOG.info("Product not used");
            }
        } finally {
            pm.done();
            processorAdapter.dispose();
        }

        final Exception[] exceptions = spatialBinner.getExceptions();
        for (Exception exception : exceptions) {
            String m = MessageFormat.format("Failed to process input slice of {0}", processorAdapter.getInputPath());
            LOG.log(Level.SEVERE, m, exception);
        }
        LOG.info(MessageFormat.format("Finishes processing of {0}  ({1} observations seen, {2} bins produced)",
                                      processorAdapter.getInputPath(),
                                      spatialBinEmitter.numObsTotal,
                                      spatialBinEmitter.numBinsTotal));
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


    static String extractProcessingGraphXml(Product product) {
        final MetadataElement metadataRoot = product.getMetadataRoot();
        final MetadataElement processingGraph = metadataRoot.getElement("Processing_Graph");
        final MetadataSerializer metadataSerializer = new MetadataSerializer();
        return metadataSerializer.toXml(processingGraph);
    }

    private static class SpatialBinEmitter implements SpatialBinConsumer {
        private Context context;
        int numObsTotal = 0;
        int numBinsTotal = 0;

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
