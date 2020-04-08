/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.l2tol3;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.ProcessingMetadata;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.calvalus.processing.l3.HadoopBinManager;
import com.bc.calvalus.processing.l3.L3SpatialBin;
import com.bc.calvalus.processing.l3.L3TemporalBin;
import com.bc.calvalus.processing.ma.TaskOutputStreamFactory;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.snap.binning.BinningContext;
import org.esa.snap.binning.DataPeriod;
import org.esa.snap.binning.SpatialBin;
import org.esa.snap.binning.SpatialBinConsumer;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.operator.SpatialProductBinner;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reads an N1 product and produces an emits (binIndex, spatialBin) pairs.
 *
 * @author Marco Zuehlke
 * @author Norman Fomferra
 */
public class L2toL3Mapper extends Mapper<NullWritable, NullWritable, LongWritable, L3SpatialBin> {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Geometry regionGeometry = GeometryUtils.createGeometry(conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));

        BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
        DataPeriod dataPeriod = HadoopBinManager.createDataPeriod(conf, binningConfig.getMinDataHour());
        BinningContext binningContext = HadoopBinManager.createBinningContext(binningConfig,
                                                                              dataPeriod,
                                                                              regionGeometry);
        final SpatialBinEmitter spatialBinEmitter = new SpatialBinEmitter(context);

        RatioCalculator ratioCalculator = null;
        String l3MeanString = conf.get("calvalus.l2tol3.l3path");
        if (l3MeanString != null) {

            Path l3MeanPath = new Path(l3MeanString);

            Map<String, String> metadata;
            Path inputDirectory = l3MeanPath.getParent();
            metadata = ProcessingMetadata.read(inputDirectory, conf);
            ProcessingMetadata.metadata2Config(metadata, conf, JobConfigNames.LEVEL3_METADATA_KEYS);
            String[] l3MeanFeatureNames = conf.getStrings(JobConfigNames.CALVALUS_L3_FEATURE_NAMES);
            Map<Long, float[]> l3MeanValues = readL3MeanValues(l3MeanPath, conf);

            ratioCalculator = new RatioCalculator(binningContext.getVariableContext(), l3MeanFeatureNames, l3MeanValues);
        }

        L2toL3SpatialBinner spatialBinner = null;
        final ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        LOG.info("processing input " + processorAdapter.getInputPath() + " ...");
        ProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        final int progressForProcessing = processorAdapter.supportsPullProcessing() ? 5 : 90;
        final int progressForBinning = processorAdapter.supportsPullProcessing() ? 90 : 20;
        pm.beginTask("Level 3", progressForProcessing + progressForBinning);
        try {
            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, progressForProcessing));
            if (product != null) {
                int productWidth = product.getSceneRasterWidth();
                spatialBinner = new L2toL3SpatialBinner(binningContext, spatialBinEmitter, ratioCalculator, productWidth);
                HashMap<Product, List<Band>> addedBands = new HashMap<>();
                long numObs = SpatialProductBinner.processProduct(product,
                                                                  spatialBinner,
                                                                  addedBands,
                                                                  SubProgressMonitor.create(pm, progressForBinning));
                if (numObs > 0L) {
                    if (ratioCalculator != null) {
                        // only do this once
                        double asymmetry = spatialBinner.getAsymmetrySum() / numObs;
                        ProductData.UTC startTime = product.getStartTime();
                        int doy = -1;
                        if (startTime != null) {
                            doy = startTime.getAsCalendar().get(Calendar.DAY_OF_YEAR);
                        }
                        String filename = product.getName() + ".asymmetry.csv";
                        try (Writer writer = new OutputStreamWriter(TaskOutputStreamFactory.createOutputStream(context, filename))) {
                            writer.write("numObs\tdoy\tasymmetry\n");
                            writer.write(numObs + "\t" + doy + "\t" + asymmetry + "\n");
                        }
                    }
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product with pixels").increment(1);
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Pixel processed").increment(numObs);
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
        if (spatialBinner != null) {
            final Exception[] exceptions = spatialBinner.getExceptions();
            for (Exception exception : exceptions) {
                String m = MessageFormat.format("Failed to process input slice of {0}", processorAdapter.getInputPath());
                LOG.log(Level.SEVERE, m, exception);
            }
        }
        // write final log entry for runtime measurements
        LOG.info(MessageFormat.format(
                "Finishes processing of {1} after {2} sec ({3} observations seen, {4} bins produced)",
                context.getTaskAttemptID(), processorAdapter.getInputPath(),
                spatialBinEmitter.numObsTotal, spatialBinEmitter.numBinsTotal));
    }

    private Map<Long, float[]> readL3MeanValues(Path l3Path, Configuration conf) throws IOException {
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(l3Path));
        LongWritable index = new LongWritable();
        L3TemporalBin l3TemporalBin = new L3TemporalBin();
        Map<Long, float[]> map = new HashMap<>();
        while (reader.next(index, l3TemporalBin)) {
            map.put(index.get(), l3TemporalBin.getFeatureValues().clone());
        }
        return map;
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
