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

package com.bc.calvalus.processing.mosaic2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.ParameterizedSplit;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.calvalus.processing.l3.HadoopBinManager;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.snap.binning.AggregatorConfig;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.logging.Logger;

/**
 * processes and reprojects one input, applies aggregators, writes micro tiles (instead of single pixels).
 *
 * @author Martin
 */
public class CubeMapper extends Mapper<NullWritable, NullWritable, CubeIndexWritable, CubeChunkWritable> {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        // read configuration (variables, tile size)

        final Configuration conf = context.getConfiguration();
        final BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
        final AggregatorConfig[] aggregatorConfigs = binningConfig.getAggregatorConfigs();
        if (aggregatorConfigs.length < 1 || ! "TOptCube".equals(aggregatorConfigs[0].getName())) {
            throw new IllegalArgumentException("configuration incomplete, aggregator TOptCube expected");
        }
        final AggregatorCube.Config aggregatorConfig = (AggregatorCube.Config) aggregatorConfigs[0];
        final String[] variableNames = aggregatorConfig.varNames.split(",");
        //final int chunkSizeT = aggregatorConfig.chunkSizeT;
        final int chunkSizeY = aggregatorConfig.chunkSizeY;
        final int chunkSizeX = aggregatorConfig.chunkSizeX;
        final boolean generateEmptyAggregate = conf.getBoolean("calvalus.generateEmptyAggregate", false);

        // open product (time index, dimensions, fill value, variable content)

        final ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        final ProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        final int progressForProcessing = processorAdapter.supportsPullProcessing() ? 5 : 90;
        final int progressForBinning = processorAdapter.supportsPullProcessing() ? 90 : 20;
        LOG.info("processing input " + processorAdapter.getInputPath() + " ...");
        pm.beginTask("Level 3", progressForProcessing + progressForBinning);
        try {
            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, progressForProcessing));
            final int productHeight = product.getSceneRasterHeight();
            final int productWidth = product.getSceneRasterWidth();
            final String[] parameters = ((ParameterizedSplit) context.getInputSplit()).getParameters();
            if (parameters.length > 2 || !"timeIndex".equals(parameters[0])) {
                throw new IllegalArgumentException("input table or parameters incomplete, parameter timeIndex expected ");
            }
            final int timeIndex = Integer.parseInt(parameters[1]);

            // TODO forward time value to a reducer, it is required to write the time variable
            // TODO forward fill value

            // loop over variables
            // loop over tiles

            int numObs = 0;
            int numBins = 0;
            for (int i = 0; i < variableNames.length; ++i) {
                final Band band = product.getBand(variableNames[i]);
                if (band == null) {
                    throw new IllegalArgumentException("variable " + variableNames[i] + " not found in input " + ((ParameterizedSplit) context.getInputSplit()).getPath());
                }
                double fillValue = band.isNoDataValueSet() ? band.getNoDataValue() : Double.NaN;
                final ProductData data = ProductData.createInstance(band.getDataType(), chunkSizeY * chunkSizeX);
                for (int ty = 0; ty < (productHeight + chunkSizeY - 1) / chunkSizeY; ++ty) {
                    final int startY = ty * chunkSizeY;
                    final int countY = Math.min(chunkSizeY, productHeight - startY);
                    for (int tx = 0; tx < (productWidth + chunkSizeX - 1) / chunkSizeX; ++tx) {
                        final int startX = tx * chunkSizeX;
                        final int countX = Math.min(chunkSizeX, productWidth - startX);
                        band.readRasterData(startX, startY, countX, countY, data);
                        // check whether some values are not fill value
                        if (generateEmptyAggregate || containsNonFillValue(data.getElems(), countY * countX, fillValue)) {
                            // determine key
                            final CubeIndexWritable key = new CubeIndexWritable((short) i, (byte) ty, (byte) tx, timeIndex);
                            final CubeChunkWritable chunk = new CubeChunkWritable(data.getElems(), countY * countX);
                            // send data
                            context.write(key, chunk);
                            numObs += countY * countX;
                            ++numBins;
                        }
                    }
                }
            }
            numObs /= variableNames.length;

            if (numObs > 0L) {
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product with pixels").increment(1);
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Pixel processed").increment(numObs);
            } else {
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product without pixels").increment(1);
            }

            LOG.info(MessageFormat.format("Finishes processing of {0}  ({1} observations seen, {2} bins produced)",
                                          processorAdapter.getInputPath(),
                                          numObs,
                                          numBins));
        } finally {
            pm.done();
            processorAdapter.dispose();
        }
    }

    private boolean containsNonFillValue(Object elems, int length, double fillValue) {
        if (elems instanceof float[]) {
            float[] values = (float[]) elems;
            if (Double.isNaN(fillValue)) {
                for (int i=0; i<length; ++i) {
                    if (! Double.isNaN(values[i])) {
                        return true;
                    }
                }
            } else {
                float fillValue1 = (float) fillValue;
                for (int i=0; i<length; ++i) {
                    if (values[i] != fillValue1) {
                        return true;
                    }
                }
            }
        } else if (elems instanceof int[]) {
            if (Double.isNaN(fillValue)) {
                return true;
            } else {
                int[] values = (int[]) elems;
                int fillValue1 = (int) fillValue;
                for (int i=0; i<length; ++i) {
                    if (values[i] != fillValue1) {
                        return true;
                    }
                }
            }
        } else if (elems instanceof short[]) {
            if (Double.isNaN(fillValue)) {
                return true;
            } else {
                short[] values = (short[]) elems;
                short fillValue1 = (short) fillValue;
                for (int i=0; i<length; ++i) {
                    if (values[i] != fillValue1) {
                        return true;
                    }
                }
            }
        } else if (elems instanceof byte[]) {
            if (Double.isNaN(fillValue)) {
                return true;
            } else {
                byte[] values = (byte[]) elems;
                byte fillValue1 = (byte) fillValue;
                for (int i=0; i<length; ++i) {
                    if (values[i] != fillValue1) {
                        return true;
                    }
                }
            }
        } else if (elems instanceof double[]) {
            double[] values = (double[]) elems;
            if (Double.isNaN(fillValue)) {
                for (int i=0; i<length; ++i) {
                    if (! Double.isNaN(values[i])) {
                        return true;
                    }
                }
            } else {
                double fillValue1 = (double) fillValue;
                for (int i=0; i<length; ++i) {
                    if (values[i] != fillValue1) {
                        return true;
                    }
                }
            }
        } else if (elems instanceof long[]) {
            if (Double.isNaN(fillValue)) {
                return true;
            } else {
                long[] values = (long[]) elems;
                long fillValue1 = (long) fillValue;
                for (int i=0; i<length; ++i) {
                    if (values[i] != fillValue1) {
                        return true;
                    }
                }
            }
        } else {
            throw new IllegalArgumentException("unexpected type of " + elems);
        }
        return false;
    }
}
