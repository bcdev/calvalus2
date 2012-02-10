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

import com.bc.calvalus.binning.BinningContext;
import com.bc.calvalus.binning.SpatialBin;
import com.bc.calvalus.binning.SpatialBinProcessor;
import com.bc.calvalus.binning.SpatialBinner;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.ProductFactory;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.binning.SpatialProductBinner;
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
        final Configuration jobConfig = context.getConfiguration();
        final L3Config l3Config = L3Config.get(jobConfig);
        final ProductFactory productFactory = new ProductFactory(jobConfig);

        final BinningContext ctx = l3Config.getBinningContext();
        final SpatialBinEmitter spatialBinEmitter = new SpatialBinEmitter(context);
        final SpatialBinner spatialBinner = new SpatialBinner(ctx, spatialBinEmitter);

        final FileSplit split = (FileSplit) context.getInputSplit();

        // write initial log entry for runtime measurements
        LOG.info(MessageFormat.format("{0} starts processing of split {1}", context.getTaskAttemptID(), split));
        final long startTime = System.nanoTime();

        Product product = productFactory.getProcessedProduct(context.getInputSplit());
        if (product != null) {
            try {
                long numObs = processProduct(product, ctx, spatialBinner, l3Config.getSuperSampling(), context);
                if (numObs > 0L) {
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product with pixels").increment(1);
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Pixel processed").increment(numObs);
                } else {
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product without pixels").increment(1);
                }
            } finally {
                product.dispose();
            }
        } else {
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product not used").increment(1);
            LOG.info("Product not used");
        }
        productFactory.dispose();

        long stopTime = System.nanoTime();

        final Exception[] exceptions = spatialBinner.getExceptions();
        for (Exception exception : exceptions) {
            String m = MessageFormat.format("Failed to process input slice of split {0}", split);
            LOG.log(Level.SEVERE, m, exception);
        }
        // write final log entry for runtime measurements
        LOG.info(MessageFormat.format("{0} stops processing of split {1} after {2} sec ({3} observations seen, {4} bins produced)",
                                      context.getTaskAttemptID(), split, (stopTime - startTime) / 1E9, spatialBinEmitter.numObsTotal, spatialBinEmitter.numBinsTotal));
    }

    static long processProduct(Product product,
                               BinningContext ctx,
                               SpatialBinner spatialBinner,
                               Integer superSampling,
                               MapContext mapContext) throws IOException, InterruptedException {
        return SpatialProductBinner.processProduct(product, spatialBinner, superSampling,
                                                   new ProductSplitProgressMonitor(mapContext));
    }


    private static class SpatialBinEmitter implements SpatialBinProcessor {
        private Context context;
        int numObsTotal = 0;
        int numBinsTotal = 0;

        public SpatialBinEmitter(Context context) {
            this.context = context;
        }

        @Override
        public void processSpatialBinSlice(BinningContext ctx, List<SpatialBin> spatialBins) throws Exception {
            for (SpatialBin spatialBin : spatialBins) {
                context.write(new LongWritable(spatialBin.getIndex()), (L3SpatialBin) spatialBin);
                numObsTotal += spatialBin.getNumObs();
                numBinsTotal++;
            }
        }
    }

    private static class ProductSplitProgressMonitor implements ProgressMonitor {
        private float totalWork;
        private final MapContext mapContext;
        private float work;

        public ProductSplitProgressMonitor(MapContext mapContext) {
            this.mapContext = mapContext;
        }

        @Override
        public void beginTask(String taskName, int totalWork) {
            this.totalWork = totalWork;
        }

        @Override
        public void worked(int delta) {
            work += delta;
            ProductSplit productSplit = (ProductSplit) mapContext.getInputSplit();
            productSplit.setProgress(Math.min(1.0f,  work / totalWork));
            try {
                // trigger progress propagation (yes, that's weird but we don't use true input formats
                // that are responsible for read progress)
                mapContext.nextKeyValue();
            } catch (IOException e) {
                // ignore
            } catch (InterruptedException e) {
                // ignore
            }
        }


        @Override
        public void done() {
        }

        @Override
        public void internalWorked(double work) {
        }

        @Override
        public boolean isCanceled() {
            return false;
        }

        @Override
        public void setCanceled(boolean canceled) {
        }

        @Override
        public void setTaskName(String taskName) {
        }

        @Override
        public void setSubTaskName(String subTaskName) {
        }
    }
}
