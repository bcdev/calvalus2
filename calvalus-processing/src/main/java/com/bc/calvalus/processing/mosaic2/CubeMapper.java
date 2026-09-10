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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
 * Processes one input, cuts into spatial chunks, streams them to reducers ordered by variable, spatial chunk, time.
 * It streams the single time value to a common reducer.
 * If this input is labelled to provide the metadata then this mapper writes all .zxxx files, y and x.
 *
 * @author MB
 */
public class CubeMapper extends Mapper<NullWritable, NullWritable, CubeIndexWritable, CubeChunkWritable> {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";
    private static final String CUBE_REFERENCE_DATE = "1970-01-01";

    int numObs = 0;
    int numBins = 0;
    ObjectMapper objectMapper;
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
        final int timeAxisLength = aggregatorConfig.timeAxisLength;
        final int yAxisLength = aggregatorConfig.yAxisLength;
        final int xAxisLength = aggregatorConfig.xAxisLength;
        final int chunkSizeT = aggregatorConfig.chunkSizeT;
        final int chunkSizeY = aggregatorConfig.chunkSizeY;
        final int chunkSizeX = aggregatorConfig.chunkSizeX;
        final String encoding = aggregatorConfig.encoding;
        final String compressorName = aggregatorConfig.compression.split(",")[0];
        final String[] compressorParameters = aggregatorConfig.compression.substring(aggregatorConfig.compression.indexOf(",") + 1).split(",");
        final String jsonFormattedCubeMetadataStr = aggregatorConfig.jsonFormattedCubeMetadataStr;
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
            if (parameters.length < 2 || !"timeIndex".equals(parameters[0])) {
                throw new IllegalArgumentException("input table or parameters incomplete, parameter timeIndex expected ");
            }
            final int timeIndex = Integer.parseInt(parameters[1]);
            final boolean writeChunksOnly = (parameters.length >= 4 && "writeChunksOnly".equals(parameters[2]))
                    ? Boolean.parseBoolean(parameters[3])
                    : true;
            final String destDir = conf.get("calvalus.output.dir");

            final MetadataCollector metadataCollector;
            final ZarrWriter zarrWriter;
            final ObjectNode zmetadata;
            final ObjectNode metadata;
            if (!writeChunksOnly) {
                objectMapper = new ObjectMapper();
                zmetadata = objectMapper.createObjectNode();
                metadata = zmetadata.putObject("metadata");
                zarrWriter = new ZarrWriter(objectMapper);
                metadataCollector = new MetadataCollector(objectMapper, CUBE_REFERENCE_DATE);
            } else {
                objectMapper = null;
                zmetadata = null;
                metadata = null;
                zarrWriter = null;
                metadataCollector = null;
            }

            // TODO forward time value to a reducer, it is required to write the time variable
            // TODO forward fill value

            // loop over variables
            // loop over tiles

            numObs = 0;
            numBins = 0;
            for (int i = 0; i < variableNames.length; ++i) {

                // stream band data to reducers

                final Band band = product.getBand(variableNames[i]);
                if (band == null) {
                    throw new IllegalArgumentException("variable " + variableNames[i] + " not found in input " + ((ParameterizedSplit) context.getInputSplit()).getPath());
                }
                double fillValue = band.isNoDataValueSet() ? band.getNoDataValue() : Double.NaN;
                
                streamBandData(
                        band, (short) i, timeIndex, fillValue, generateEmptyAggregate,
                        productHeight, productWidth, chunkSizeY, chunkSizeX,
                        context
                );

                // write band metadata to zarr

                if (!writeChunksOnly) {

                    final ObjectNode zarray = metadataCollector.collectZarrayContent(
                            variableNames[i], band, fillValue, encoding, compressorName, compressorParameters, 
                            timeAxisLength, yAxisLength, xAxisLength, chunkSizeT, chunkSizeY, chunkSizeX, 
                            metadata
                    );
                    zarrWriter.writeJsonFile(destDir, variableNames[i],  ".zarray", zarray);

                    final ObjectNode zattrs = metadataCollector.collectZattrsContent(
                            variableNames[i], band, metadata
                    );
                    zarrWriter.writeJsonFile(destDir, variableNames[i],  ".zattrs", zattrs);
                }
            }
            // count 1 per pixel only
            numObs /= variableNames.length;

            if (! writeChunksOnly) {

                // write y, x, and metadata of time, spatial_ref, global metadata, .zmetadata

                final ObjectNode zarrayY = metadataCollector.collectXYzarray("y", metadata, product);
                zarrWriter.writeJsonFile(destDir, "y",  ".zarray", zarrayY);
                final ObjectNode zattrsY = metadataCollector.collectXYzattrs("y", metadata);
                zarrWriter.writeJsonFile(destDir, "y",  ".zattrs", zattrsY);
                zarrWriter.writeXYValuesToZarr("y", product, destDir);

                final ObjectNode zarrayX = metadataCollector.collectXYzarray("x", metadata, product);
                zarrWriter.writeJsonFile(destDir, "x",  ".zarray", zarrayX);
                final ObjectNode zattrsX = metadataCollector.collectXYzattrs("x", metadata);
                zarrWriter.writeJsonFile(destDir, "x",  ".zattrs", zattrsX);
                zarrWriter.writeXYValuesToZarr("x", product, destDir);

                final ObjectNode zarrayTime = metadataCollector.collectTarray(timeAxisLength, chunkSizeT, metadata);
                zarrWriter.writeJsonFile(destDir, "time",  ".zarray", zarrayTime);
                final ObjectNode zattrsTime = metadataCollector.collectTattrs(metadata);
                zarrWriter.writeJsonFile(destDir, "time",  ".zattrs", zattrsTime);

                final ObjectNode zarraySpatialRef = metadataCollector.collectCRSarray(metadata);
                zarrWriter.writeJsonFile(destDir, "spatial_ref",  ".zarray", zarraySpatialRef);
                final ObjectNode zattrsSpatialRef = metadataCollector.collectCRSattrs(product, metadata);
                zarrWriter.writeJsonFile(destDir, "spatial_ref",  ".zattrs", zattrsSpatialRef);

                final ObjectNode zgroup = metadataCollector.collectZgroup(metadata);
                zarrWriter.writeJsonFile(destDir,".zgroup", zgroup);
                final ObjectNode zattrsGlobal = metadataCollector.collectGlobalMetadata(jsonFormattedCubeMetadataStr, zmetadata);
                zarrWriter.writeJsonFile(destDir, ".zattrs", zattrsGlobal);

                zmetadata.put("zarr_consolidated_format", 1);
                zarrWriter.writeJsonFile(destDir, ".zmetadata", zmetadata);
            }

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

    private void streamBandData(Band band, short i, int timeIndex, double fillValue, boolean generateEmptyAggregate, int productHeight, int productWidth, int chunkSizeY, int chunkSizeX, Mapper<NullWritable, NullWritable, CubeIndexWritable, CubeChunkWritable>.Context context) throws IOException, InterruptedException {
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
                    final CubeIndexWritable key = new CubeIndexWritable(i, (byte) ty, (byte) tx, timeIndex);
                    final CubeChunkWritable chunk = new CubeChunkWritable(data.getElems(), countY * countX);
                    // send data
                    context.write(key, chunk);
                    numObs += countY * countX;
                    ++numBins;
                }
            }
        }
    }

     private static boolean containsNonFillValue(Object elems, int length, double fillValue) {
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
