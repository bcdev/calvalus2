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

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.MetadataSerializer;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
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

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads an N1 product and produces an emits (binIndex, spatialBin) pairs.
 *
 * @author Marco Zuehlke
 * @author Norman Fomferra
 */
public class L3Mapper extends Mapper<NullWritable, NullWritable, LongWritable, L3SpatialBin> {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Geometry regionGeometry = GeometryUtils.createGeometry(conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));

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
                    //
                    final String metaXml = extractProcessingGraphXml(product);
                    context.write(new LongWritable(L3SpatialBin.METADATA_MAGIC_NUMBER), new L3SpatialBin(metaXml));

                } else {
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product without pixels").increment(1);
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
