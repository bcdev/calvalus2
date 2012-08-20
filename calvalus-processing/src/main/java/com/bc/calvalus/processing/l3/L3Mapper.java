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

import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import org.esa.beam.binning.BinningContext;
import org.esa.beam.binning.SpatialBin;
import org.esa.beam.binning.SpatialBinConsumer;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.hadoop.ProductSplitProgressMonitor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.beam.binning.SpatialBinner;
import org.esa.beam.binning.operator.SpatialProductBinner;
import org.esa.beam.framework.datamodel.Product;

import java.io.IOException;
import java.text.MessageFormat;
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
        final L3Config l3Config = L3Config.get(context.getConfiguration());
        final BinningContext ctx = l3Config.createBinningContext();
        final SpatialBinEmitter spatialBinEmitter = new SpatialBinEmitter(context);
        final SpatialBinner spatialBinner = new SpatialBinner(ctx, spatialBinEmitter);
        final ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        ProgressMonitor pm = new ProductSplitProgressMonitor(context);
        pm.beginTask("Level 3", 100);
        try {
            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, 5));
            if (product != null) {
                long numObs = SpatialProductBinner.processProduct(product,
                                                                  spatialBinner,
                                                                  l3Config.getSuperSampling(),
                                                                  SubProgressMonitor.create(pm, 95));
                if (numObs > 0L) {
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

        final Exception[] exceptions = spatialBinner.getExceptions();
        for (Exception exception : exceptions) {
            String m = MessageFormat.format("Failed to process input slice of {0}", processorAdapter.getInputPath());
            LOG.log(Level.SEVERE, m, exception);
        }
        // write final log entry for runtime measurements
        LOG.info(MessageFormat.format("Finishes processing of {1} after {2} sec ({3} observations seen, {4} bins produced)",
                                      context.getTaskAttemptID(), processorAdapter.getInputPath(),
                                      spatialBinEmitter.numObsTotal, spatialBinEmitter.numBinsTotal));
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
