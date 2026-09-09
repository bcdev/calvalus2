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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.snap.binning.AggregatorConfig;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.SampleCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.geotools.referencing.operation.transform.AffineTransform2D;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
                       
    int numObs = 0;
    int numBins = 0;

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

            final ObjectMapper objectMapper;
            final ObjectNode zmetadata;
            final ObjectNode metadata;
            if (!writeChunksOnly) {
                objectMapper = new ObjectMapper();
                zmetadata = objectMapper.createObjectNode();
                metadata = zmetadata.putObject("metadata");
            } else {
                objectMapper = null;
                zmetadata = null;
                metadata = null;
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

                    // write .zarray

                    final ObjectNode zarray = collectZarrayContent(
                            variableNames[i], band, fillValue, encoding, compressorName, compressorParameters, 
                            timeAxisLength, yAxisLength, xAxisLength, chunkSizeT, chunkSizeY, chunkSizeX, 
                            metadata
                    );
                    final String destination = destDir + "/" + variableNames[i] + "/.zarray";
                    Files.createDirectories(Paths.get(destDir + "/" + variableNames[i]));
                    try (BufferedWriter out = new BufferedWriter(new FileWriter(destination))) {
                        objectMapper.writeValue(out, zarray);
                    }

                    // write .zattrs

                    final ObjectNode zattrs = collectZattrsContent(
                            variableNames[i], band, metadata
                    );
                    final String destination2 = destDir + "/" + variableNames[i] + "/.zattrs";
                    try (BufferedWriter out = new BufferedWriter(new FileWriter(destination2))) {
                        objectMapper.writeValue(out, zattrs);
                    }
                }
            }
            numObs /= variableNames.length;

            if (! writeChunksOnly) {
                                
                // TODO time, y, x

                final ObjectNode zarraySpatialRef = metadata.putObject("spatial_ref/.zarray");
                zarraySpatialRef.putArray("chunks");
                zarraySpatialRef.putNull("compressor");
                zarraySpatialRef.put("dtype", "|i1");
                zarraySpatialRef.put("fill_value", 0);
                zarraySpatialRef.putNull("filters");
                zarraySpatialRef.put("order", "C");
                zarraySpatialRef.putArray("shape");
                zarraySpatialRef.put("zarr_format", 2);

                final String destination6 = destDir + "/spatial_ref/.zarray";
                Files.createDirectories(Paths.get(destDir+ "/spatial_ref"));
                try (BufferedWriter out = new BufferedWriter(new FileWriter(destination6))) {
                    objectMapper.writeValue(out, zarraySpatialRef);
                }

                final ObjectNode zattrsSpatialRef = metadata.putObject("spatial_ref/.zattrs");
                zattrsSpatialRef.putArray("_ARRAY_DIMENSIONS");
                zattrsSpatialRef.put("crs_wkt", product.getSceneGeoCoding().getMapCRS().toWKT());
                final AffineTransform2D imageToMapTransform = (AffineTransform2D) product.getSceneGeoCoding().getImageToMapTransform();
                double[] t = new double[6];
                imageToMapTransform.getMatrix(t);
                zattrsSpatialRef.put("i2m", String.format("%f,%f,%f,%f,%f,%f", t[0],t[1],t[2],t[3],t[4],t[5]));

                final String destination7 = destDir + "/spatial_ref/.zattrs";
                try (BufferedWriter out = new BufferedWriter(new FileWriter(destination7))) {
                    objectMapper.writeValue(out, zattrsSpatialRef);
                }

                // write .zgroup

                final ObjectNode zgroup = metadata.putObject(".zgroup");
                zgroup.put("zarr_format", 2);
                final String destination3 = destDir + "/.zgroup";
                Files.createDirectories(Paths.get(destDir));
                try (BufferedWriter out = new BufferedWriter(new FileWriter(destination3))) {
                    objectMapper.writeValue(out, zgroup);
                }

                // write .zattrs

                final ObjectNode zattrsGlobal = zmetadata.putObject(".zattrs");
                for (String attr : product.getMetadataRoot().getAttributeNames()) {
                    zattrsGlobal.put(attr, product.getMetadataRoot().getAttributeString(attr));
                }
                final String destination4 = destDir + "/.zattrs";
                //Files.createDirectories(Paths.get(destDir));
                try (BufferedWriter out = new BufferedWriter(new FileWriter(destination4))) {
                    objectMapper.writeValue(out, zattrsGlobal);
                }

                // write .zmetadata

                zmetadata.put("zarr_consolidated_format", 1);
                final String destination5 = destDir + "/.zmetadata";
                //Files.createDirectories(Paths.get(destDir));
                try (BufferedWriter out = new BufferedWriter(new FileWriter(destination5))) {
                    objectMapper.writeValue(out, zmetadata);
                }
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

    private ObjectNode collectZattrsContent(String variableName, Band band, ObjectNode metadata) {
        final ObjectNode zattrs = metadata.putObject(variableName + "/" + ".zattrs");
        final ArrayNode dims = zattrs.putArray("_ARRAY_DIMENSIONS");
        dims.add("time");
        dims.add("y");   // TODO is it sometimes lat and lon?
        dims.add("x");
        zattrs.put("grid_mapping", "spatial_ref");
        if (band.getDescription() != null) {
            zattrs.put("description", band.getDescription());
        }
        if (band.getFlagCoding() != null) {
            zattrs.put("flag_masks", flagMasksOf(band.getFlagCoding()));
            zattrs.put("flag_meanings", flagMeaningsOf(band.getFlagCoding()));
        } else if (band.getIndexCoding() != null) {
            zattrs.put("flag_values", flagMasksOf(band.getIndexCoding()));
            zattrs.put("flag_meanings", flagMeaningsOf(band.getIndexCoding()));
        }
        if (band.getSpectralWavelength() > 0.0f) {
            zattrs.put("spectral_wavelength", band.getSpectralWavelength());
        }
        zattrs.put("grid_mapping", "spatial_ref");
        return zattrs;
    }

    private ObjectNode collectZarrayContent(
            String variableName, Band band, double fillValue,
            String encoding, String compressorName, String[] compressorParameters,
            int timeAxisLength, int yAxisLength, int xAxisLength, int chunkSizeT, int chunkSizeY, int chunkSizeX,
            ObjectNode metadata
    ) {
        final ObjectNode zarray =  metadata.putObject(variableName + "/" + ".zarray");
        final ArrayNode chunks = zarray.putArray("chunks");
        chunks.add(chunkSizeT);
        chunks.add(chunkSizeY);
        chunks.add(chunkSizeX);
        final ObjectNode compressor = zarray.putObject("compressor");
        compressor.put("id", compressorName);
        for (String parameter : compressorParameters) {
            compressor.put(parameter.split(":")[0], parameter.split(":")[1]);
        }
        zarray.put("dtype", zarrEncodingOf(encoding) + zarrTypeOf(band.getDataType()));
        if (band.isNoDataValueSet()) {
            if (Double.isNaN(fillValue)) {
                zarray.put("fill_value", "NaN");
            } else {
                zarray.put("fill_value", fillValue);
            }
        }
        zarray.putNull("filters");
        zarray.put("order", "C");
        final ArrayNode shape = zarray.putArray("shape");
        chunks.add(timeAxisLength);
        chunks.add(yAxisLength);
        chunks.add(xAxisLength);
        zarray.put("zarr_format", 2);
        return zarray;
    }

    private String zarrEncodingOf(String encoding) {
        if ("littleendian".equals(encoding)) {
            return "<";
        } else {
            return ">";
        }
    }

    private String flagMasksOf(SampleCoding flagCoding) {
        StringBuffer accu = new StringBuffer();
        for (int i=0; i<flagCoding.getSampleCount(); ++i) {
            if (i>0) {
                accu.append(",");
            }
            accu.append(flagCoding.getSampleValue(i));
        }
        return accu.toString();
    }

    private String flagMeaningsOf(SampleCoding flagCoding) {
        StringBuffer accu = new StringBuffer();
        for (int i=0; i<flagCoding.getSampleCount(); ++i) {
            if (i>0) {
                accu.append(",");
            }
            accu.append(flagCoding.getSampleName(i));
        }
        return accu.toString();
    }

    private String zarrTypeOf(int dataType) {
        switch (dataType) {
            case ProductData.TYPE_FLOAT32:
                return "f4";
            case ProductData.TYPE_INT32:
                return "i4";
            case ProductData.TYPE_INT16:
                return "i2";
            case ProductData.TYPE_INT8:
                return "b";
            case ProductData.TYPE_FLOAT64:
                return "f8";
            case ProductData.TYPE_INT64:
                return "i8";
            default:
                throw new IllegalArgumentException("unknown dtype code " + dataType);
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
