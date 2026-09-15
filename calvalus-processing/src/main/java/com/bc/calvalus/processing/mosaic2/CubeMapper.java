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
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.geotools.referencing.CRS;

import java.io.IOException;
import java.nio.ByteOrder;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Purpose of this mapper is to generate time-chunked data cubes from a time series of inputs, e.g. daily coverages.
 *
 * The mapper expects an input product in the target projection with the target variables.
 * Parameters per product (table input format) are the time index and whether this is the selected input to write
 * the metadata of the cube.
 * Parameters common to all calls are chunk sizes and dimensions in T, Y, X, byte order, compression, global metadata.
 *
 * The following classes implement the functions to generate the cube:
 * - CubeMapper cuts one input into spatial chunks, streams them to reducers ordered by variable, spatial chunk, time.
 * - CubePartitioner sorts the keys of variable, y tile, x tile, time and partitions them by time chunk
 * - CubeReducer collects the contributions for one chunk in temporal order, writes it, initialises for the next chunk
 * - CubeIndexWritable container for the key of variable, y tile, x tile, time index
 * - CubeDataWritable container for the chunk contribution as float array on the mapper side and byte array at reducer
 * - AggregatorCube container for the common parameters, an aggregator only to pass it in a kind of level 3 request
 * - MetadataCollector functions to create JSON record for .zarray, .zattrs, .zmetadata
 * - ZarrWriter functions to write JSON record to file, to write coordinates, to compress and write data chunk
 *
 * Key is variable index, y tile index, x tile index, time index.
 * The chunk is written as byte array on the mapper side, considering byte order for the target cube already.
 * The value transmitted from mapper to reducer is a quadrupel of num bytes per value, num bytes of the transmitted
 * array, fill value encoded bytes, data encoded as bytes.
 * The quadrupel is compressed for transfer and will be uncompressed on the reducer side. This is lzw level 1 for speed.
 *
 * The time of the input is streamed as a separate key-value pair.
 * The key is #variables, 0, 0, time index.
 * The value is a quadrupel of 8, 8, the bytes for double -1 as fill value, and the bytes for the double with the
 * number of days between 2000-01-01 and the acquisition time of the input product of this mapper.
 * If this input is labelled to provide the metadata then this mapper writes all .zxxx files, y and x.
 *
 * @author MB
 */
public class CubeMapper extends Mapper<NullWritable, NullWritable, CubeIndexWritable, CubeChunkWritable> {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";
    private static final String CUBE_REFERENCE_DATE = "2000-01-01";

    int numObs = 0;
    int numBins = 0;

    /**
     * Mapper implementation that processes an input split with one product.
     * @param context  container of input split and configuration
     * @throws IOException  if access to input or writing of zarr files fails
     * @throws IllegalArgumentException  if input and parameters do not match expectations
     * @throws InterruptedException  if streaming fails
     */
    @Override
    public void run(Context context) throws IOException, InterruptedException {

        // read configuration (variables, tile size, destDir)

        final Configuration conf = context.getConfiguration();
        final BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
        final AggregatorConfig[] aggregatorConfigs = binningConfig.getAggregatorConfigs();
        if (aggregatorConfigs.length < 1 || ! "TimeChunkedCube".equals(aggregatorConfigs[0].getName())) {
            throw new IllegalArgumentException("configuration incomplete, aggregator TimeChunkedCube expected");
        }
        final TimeChunkedCubeAggregator.Config aggregatorConfig = (TimeChunkedCubeAggregator.Config) aggregatorConfigs[0];
        final String[] variableNames = aggregatorConfig.varNames.split(",");
        final int timeShape = aggregatorConfig.timeShape;
        final int yShape = aggregatorConfig.yShape;
        final int xShape = aggregatorConfig.xShape;
        final int timeChunks = aggregatorConfig.timeChunks;
        final int yChunks = aggregatorConfig.yChunks;
        final int xChunks = aggregatorConfig.xChunks;
        final ByteOrder byteOrder = "bigendian".equals(aggregatorConfig.byteOrder) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
        final String compressorName = aggregatorConfig.compression.split(",")[0];
        final Map<String,String> compressorParameters = new HashMap<>();
        for (String pair: aggregatorConfig.compression.substring(aggregatorConfig.compression.indexOf(",") + 1).split(",")) {
            String[] elements = pair.split(":");
            compressorParameters.put(elements[0], elements[1]);
        }
        final String cubeGlobalMetadata = aggregatorConfig.cubeMetadata;
        final boolean generateEmptyAggregate = conf.getBoolean("calvalus.generateEmptyAggregate", false);
        final String destDir = conf.get("calvalus.output.dir");

        // open product (time index, date, dimensions, fill value, variable content)

        final ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        final ProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        final int progressForProcessing = processorAdapter.supportsPullProcessing() ? 5 : 90;
        final int progressForBinning = processorAdapter.supportsPullProcessing() ? 90 : 20;
        LOG.info("processing input " + processorAdapter.getInputPath() + " ...");
        pm.beginTask("producing chunks", progressForProcessing + progressForBinning);
        try {
            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, progressForProcessing));
            final int productHeight = product.getSceneRasterHeight();
            final int productWidth = product.getSceneRasterWidth();
            if (productHeight != yShape || productWidth != xShape) {
                throw new IllegalArgumentException(
                        "input " + processorAdapter.getInputPath().getName()
                                + " extent " + productHeight + "*" + productWidth
                                + " does not match cube " + yShape + "*" + xShape);
            }
            final String[] parameters = ((ParameterizedSplit) context.getInputSplit()).getParameters();
            if (parameters.length < 2 || !"timeIndex".equals(parameters[0])) {
                throw new IllegalArgumentException("input table or parameters of " + processorAdapter.getInputPath() + " incomplete, parameter timeIndex expected ");
            }
            final int timeIndex = Integer.parseInt(parameters[1]);
            final boolean writeChunksOnly = (parameters.length >= 4 && "writeChunksOnly".equals(parameters[2]))
                    ? Boolean.parseBoolean(parameters[3])
                    : true;

            final double mjd = product.getStartTime().getMJD();
            final boolean isGeographicProjection = isGeographic(product.getSceneGeoCoding());

            LOG.info(MessageFormat.format("Processing of {0} with time index {1} date {2} started",
                                          processorAdapter.getInputPath(),
                                          timeIndex,
                                          product.getStartTime().format()));



            final MetadataCollector metadataCollector;
            final ZarrWriter zarrWriter;
            final ObjectNode zmetadata;
            final ObjectNode metadataRoot;
            final String dimY;
            final String dimX;
            if (!writeChunksOnly) {
                ObjectMapper jsonFactory = new ObjectMapper();
                zmetadata = jsonFactory.createObjectNode();
                metadataRoot = zmetadata.putObject("metadata");
                zarrWriter = new ZarrWriter(jsonFactory, byteOrder, compressorName, compressorParameters, conf, destDir);
                metadataCollector = new MetadataCollector(jsonFactory, CUBE_REFERENCE_DATE);
                dimY = isGeographicProjection ? "lat" : "y";
                dimX = isGeographicProjection ? "lon" : "x";
            } else {
                zmetadata = null;
                metadataRoot = null;
                zarrWriter = null;
                metadataCollector = null;
                dimY = null;
                dimX = null;
            }

            // stream time value with key num_variables x 0 x 0 x timeIndex

            CubeIndexWritable timeKey = new CubeIndexWritable((short)variableNames.length, (byte)0, (byte)0, timeIndex);
            CubeChunkWritable timeValue = new CubeChunkWritable(new double[] { mjd }, 1, byteOrder, -1.0);
            context.write(timeKey, timeValue);

            LOG.info("Time " + timeKey + " value " + mjd + " streamed");

            // loop over variables and over their tiles

            numObs = 0;
            numBins = 0;
            for (int variableIndex = 0; variableIndex < variableNames.length; ++variableIndex) {

                // stream band data to reducers

                final Band band = product.getBand(variableNames[variableIndex]);
                if (band == null) {
                    throw new IllegalArgumentException("variable " + variableNames[variableIndex] + " not found in input " + ((ParameterizedSplit) context.getInputSplit()).getPath());
                }
                double fillValue = band.isNoDataValueSet() ? band.getNoDataValue() : Double.NaN;
                
                final int numTiles = streamBandData(
                        band, (short) variableIndex, timeIndex, fillValue, generateEmptyAggregate, byteOrder,
                        productHeight, productWidth, yChunks, xChunks,
                        context
                );

                LOG.info("Variable " + variableNames[variableIndex] + " has " + numTiles + " non-empty tiles that have been streamed");

                // write band metadata to zarr

                if (!writeChunksOnly) {

                    final ObjectNode zarray = metadataCollector.collectVariableArray(
                            variableNames[variableIndex], band, fillValue, byteOrder, compressorName, compressorParameters,
                            timeShape, yShape, xShape, timeChunks, yChunks, xChunks,
                            metadataRoot
                    );
                    zarrWriter.writeJsonFile(destDir, variableNames[variableIndex],  ".zarray", zarray);

                    final ObjectNode zattrs = metadataCollector.collectVariableAttrs(
                            variableNames[variableIndex], band, metadataRoot, dimY, dimX
                    );
                    zarrWriter.writeJsonFile(destDir, variableNames[variableIndex],  ".zattrs", zattrs);
                }

                pm.worked((progressForProcessing + progressForBinning) / variableNames.length);
            }
            // count 1 per pixel only
            numObs /= variableNames.length;

            // maintain counters

            if (numObs > 0L) {
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product with pixels").increment(1);
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Pixel processed").increment(numObs);
            } else {
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product without pixels").increment(1);
            }

            // write y, x, and metadata of time, spatial_ref, global metadata, .zmetadata

            if (! writeChunksOnly) {

                final ObjectNode zattrsY;
                final ObjectNode zattrsX;
                if (isGeographicProjection) {
                    zattrsY = metadataCollector.collectLatLonAttrs(dimY, metadataRoot);
                    zattrsX = metadataCollector.collectLatLonAttrs(dimX, metadataRoot);
                    zarrWriter.writeLatLonValuesToZarr(dimY, product, destDir);
                    zarrWriter.writeLatLonValuesToZarr(dimX, product, destDir);
                } else {
                    zattrsY = metadataCollector.collectXyAttrs("y", metadataRoot);
                    zattrsX = metadataCollector.collectXyAttrs("x", metadataRoot);
                    zarrWriter.writeXYValuesToZarr("y", product, destDir);
                    zarrWriter.writeXYValuesToZarr("x", product, destDir);
                }

                final ObjectNode zarrayY = metadataCollector.collectCoordinateArray(dimY, product.getSceneRasterHeight(), byteOrder, metadataRoot);
                zarrWriter.writeJsonFile(destDir, dimY, ".zarray", zarrayY);
                zarrWriter.writeJsonFile(destDir, dimY, ".zattrs", zattrsY);
                final ObjectNode zarrayX = metadataCollector.collectCoordinateArray(dimX, product.getSceneRasterWidth(), byteOrder, metadataRoot);
                zarrWriter.writeJsonFile(destDir, dimX, ".zarray", zarrayX);
                zarrWriter.writeJsonFile(destDir, dimX, ".zattrs", zattrsX);

                final ObjectNode zarrayTime = metadataCollector.collectTimeArray(timeShape, timeChunks, byteOrder, metadataRoot);
                zarrWriter.writeJsonFile(destDir, "time",  ".zarray", zarrayTime);
                final ObjectNode zattrsTime = metadataCollector.collectTimeAttrs(metadataRoot);
                zarrWriter.writeJsonFile(destDir, "time",  ".zattrs", zattrsTime);

                final ObjectNode zarraySpatialRef = metadataCollector.collectCrsArray(metadataRoot);
                zarrWriter.writeJsonFile(destDir, "spatial_ref",  ".zarray", zarraySpatialRef);
                final ObjectNode zattrsSpatialRef = metadataCollector.collectCrsAttrs(product, metadataRoot);
                zarrWriter.writeJsonFile(destDir, "spatial_ref",  ".zattrs", zattrsSpatialRef);

                final ObjectNode zgroup = metadataCollector.collectGroup(metadataRoot);
                zarrWriter.writeJsonFile(destDir,".zgroup", zgroup);
                final ObjectNode zattrsGlobal = metadataCollector.collectGlobalMetadata(cubeGlobalMetadata, zmetadata);
                zarrWriter.writeJsonFile(destDir, ".zattrs", zattrsGlobal);

                // .zmetadata must be in the end, after all variable metadata is collected
                zmetadata.put("zarr_consolidated_format", 1);
                zarrWriter.writeJsonFile(destDir, ".zmetadata", zmetadata);

                LOG.info("Zarr metadata files and coordinates written");
            }

            LOG.info(MessageFormat.format("Processing of {0} finished, {1} observations seen, {2} bins produced",
                                          processorAdapter.getInputPath(),
                                          numObs,
                                          numBins));
        } finally {
            pm.done();
            processorAdapter.dispose();
        }
    }

    protected int streamBandData(
            Band band,
            short variableIndex, int timeIndex,
            double fillValue, boolean generateEmptyAggregate,
            ByteOrder byteOrder,
            int productHeight, int productWidth, int chunkSizeY, int chunkSizeX,
            Mapper<NullWritable, NullWritable, CubeIndexWritable, CubeChunkWritable>.Context context) throws IOException, InterruptedException {
        final ProductData data = ProductData.createInstance(band.getDataType(), chunkSizeY * chunkSizeX);
        int tilesCounter = 0;
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
                    final CubeIndexWritable key = new CubeIndexWritable(variableIndex, (byte) ty, (byte) tx, timeIndex);
                    final CubeChunkWritable chunk = new CubeChunkWritable(data.getElems(), countY * countX, byteOrder, fillValue);
                    // send data
                    context.write(key, chunk);
                    numObs += countY * countX;
                    ++numBins;
                    ++tilesCounter;
                }
            }
        }
        return tilesCounter;
    }

    private static boolean isGeographic(GeoCoding sceneGeoCoding) {
        if (! (sceneGeoCoding instanceof CrsGeoCoding)) {
            throw new IllegalArgumentException("CRS geocoding of input expected, found " + sceneGeoCoding);
            //return false;
        }
        final String crsString = CRS.toSRS(((CrsGeoCoding) sceneGeoCoding).getMapCRS(), true);
        return "4326".equals(crsString) || "84".equals(crsString) || crsString.startsWith("WGS84");
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
