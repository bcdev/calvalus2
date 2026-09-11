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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.SampleCoding;
import org.geotools.referencing.operation.transform.AffineTransform2D;

import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Processes one input, cuts into spatial chunks, streams them to reducers ordered by variable, spatial chunk, time.
 * It streams the single time value to a common reducer.
 * If this input is labelled to provide the metadata then this mapper writes all .zxxx files, y and x.
 *
 * @author MB
 */
public class MetadataCollector extends Mapper<NullWritable, NullWritable, CubeIndexWritable, CubeChunkWritable> {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private final String cubeReferenceDate;

    private final ObjectMapper objectMapper;

    public MetadataCollector(ObjectMapper objectMapper, String cubeReferenceDate) {
        this.objectMapper = objectMapper;
        this.cubeReferenceDate = cubeReferenceDate;
    }

    public ObjectNode collectGlobalMetadata(String jsonFormattedCubeMetadataStr, ObjectNode zmetadata) throws JsonProcessingException {
        final ObjectNode zattrsGlobal = zmetadata.putObject(".zattrs");
        if (jsonFormattedCubeMetadataStr != null && jsonFormattedCubeMetadataStr.length() > 0) {
            final JsonNode configuredMetadata = objectMapper.readTree(jsonFormattedCubeMetadataStr);
            for (Iterator<Map.Entry<String, JsonNode>> it = configuredMetadata.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> pair = it.next();
                JsonNode node = pair.getValue();
                if (node.isFloat()) {
                    zattrsGlobal.put(pair.getKey(), pair.getValue().floatValue());
                } else if (node.isInt()) {
                    zattrsGlobal.put(pair.getKey(), pair.getValue().intValue());
                } else if (node.isBoolean()) {
                    zattrsGlobal.put(pair.getKey(), pair.getValue().booleanValue());
                } else {
                    zattrsGlobal.put(pair.getKey(), pair.getValue().asText());
                }
            }
        }
        return zattrsGlobal;
    }

    public ObjectNode collectZgroup(ObjectNode metadata) {
        final ObjectNode zgroup = metadata.putObject(".zgroup");
        zgroup.put("zarr_format", 2);
        return zgroup;
    }

    public ObjectNode collectCRSattrs(Product product, ObjectNode metadata) {
        final ObjectNode zattrsSpatialRef = metadata.putObject("spatial_ref/.zattrs");
        zattrsSpatialRef.putArray("_ARRAY_DIMENSIONS");
        zattrsSpatialRef.put("crs_wkt", product.getSceneGeoCoding().getMapCRS().toWKT());
        final AffineTransform2D imageToMapTransform = (AffineTransform2D) product.getSceneGeoCoding().getImageToMapTransform();
        double[] t = new double[6];
        imageToMapTransform.getMatrix(t);
        zattrsSpatialRef.put("i2m", String.format("%f,%f,%f,%f,%f,%f", t[0],t[1],t[2],t[3],t[4],t[5]));
        return zattrsSpatialRef;
    }

    public ObjectNode collectCRSarray(ObjectNode metadata) {
        final ObjectNode zarraySpatialRef = metadata.putObject("spatial_ref/.zarray");
        zarraySpatialRef.putArray("chunks");
        zarraySpatialRef.putNull("compressor");
        zarraySpatialRef.put("dtype", "|i1");
        zarraySpatialRef.put("fill_value", 0);
        zarraySpatialRef.putNull("filters");
        zarraySpatialRef.put("order", "C");
        zarraySpatialRef.putArray("shape");
        zarraySpatialRef.put("zarr_format", 2);
        return zarraySpatialRef;
    }

    public ObjectNode collectTattrs(ObjectNode metadata) {
        final ObjectNode zattrsTime = metadata.putObject("time/.zattrs");
        final ArrayNode tArrayDimensions = zattrsTime.putArray("_ARRAY_DIMENSIONS");
        tArrayDimensions.add("time");
        zattrsTime.put("calendar", "proleptic_gregorian");
        zattrsTime.put("long_name", "time");
        zattrsTime.put("standard_name", "time");
        zattrsTime.put("units", "days since " + cubeReferenceDate);
        return zattrsTime;
    }

    public ObjectNode collectTarray(int timeAxisLength, int chunkSizeT, ByteOrder byteOrder, ObjectNode metadata) {
        final ObjectNode zarrayTime = metadata.putObject("time/.zarray");
        final ArrayNode tChunks = zarrayTime.putArray("chunks");
        tChunks.add(chunkSizeT);
        zarrayTime.putNull("compressor");
        zarrayTime.put("dtype", zarrEncodingOf(byteOrder) + "i8");
        zarrayTime.put("fill_value", -1);
        zarrayTime.putNull("filters");
        zarrayTime.put("order", "C");
        final ArrayNode shapeT = zarrayTime.putArray("shape");
        shapeT.add(timeAxisLength);
        zarrayTime.put("zarr_format", 2);
        return zarrayTime;
    }

    public ObjectNode collectXYzattrs(String variableName, ObjectNode metadata) {
        final ObjectNode zattrs = metadata.putObject(variableName + "/.zattrs");
        final ArrayNode arrayDimensions = zattrs.putArray("_ARRAY_DIMENSIONS");
        arrayDimensions.add(variableName);
        zattrs.put("long_name", variableName + " coordinate of projection");
        zattrs.put("standard_name", "projection_" + variableName + "_coordinate");
        zattrs.put("units", "m");
        return zattrs;
    }

    public ObjectNode collectXYzarray(String variableName, int size, ByteOrder byteOrder, ObjectNode metadata) {
        final ObjectNode zarray = metadata.putObject(variableName + "/.zarray");
        final ArrayNode chunks = zarray.putArray("chunks");
        chunks.add(size);
        zarray.putNull("compressor");
        zarray.put("dtype", zarrEncodingOf(byteOrder) + "f8");
        zarray.put("fill_value", Double.NaN);
        zarray.putNull("filters");
        zarray.put("order", "C");
        final ArrayNode shape = zarray.putArray("shape");
        shape.add(size);
        zarray.put("zarr_format", 2);
        return zarray;
    }

    public ObjectNode collectZattrsContent(String variableName, Band band, ObjectNode metadata) {
        final ObjectNode zattrs = metadata.putObject(variableName + "/.zattrs");
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

    public ObjectNode collectZarrayContent(
            String variableName, Band band, double fillValue,
            ByteOrder byteOrder, String compressorName, Map<String,String> compressorParameters,
            int timeAxisLength, int yAxisLength, int xAxisLength, int chunkSizeT, int chunkSizeY, int chunkSizeX,
            ObjectNode metadata
    ) {
        final ObjectNode zarray =  metadata.putObject(variableName + "/.zarray");
        final ArrayNode chunks = zarray.putArray("chunks");
        chunks.add(chunkSizeT);
        chunks.add(chunkSizeY);
        chunks.add(chunkSizeX);
        final ObjectNode compressor = zarray.putObject("compressor");
        compressor.put("id", compressorName);
        for (Map.Entry<String,String> parameter : compressorParameters.entrySet()) {
            compressor.put(parameter.getKey(), parameter.getValue());
        }
        zarray.put("dtype", zarrEncodingOf(byteOrder) + zarrTypeOf(band.getDataType()));
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
        shape.add(timeAxisLength);
        shape.add(yAxisLength);
        shape.add(xAxisLength);
        zarray.put("zarr_format", 2);
        return zarray;
    }

    private static String zarrEncodingOf(ByteOrder byteOrder) {
        if (byteOrder == ByteOrder.BIG_ENDIAN) {
            return ">";
        } else {
            return "<";
        }
    }

    private static String flagMasksOf(SampleCoding flagCoding) {
        StringBuffer accu = new StringBuffer();
        for (int i=0; i<flagCoding.getSampleCount(); ++i) {
            if (i>0) {
                accu.append(",");
            }
            accu.append(flagCoding.getSampleValue(i));
        }
        return accu.toString();
    }

    private static String flagMeaningsOf(SampleCoding flagCoding) {
        StringBuffer accu = new StringBuffer();
        for (int i=0; i<flagCoding.getSampleCount(); ++i) {
            if (i>0) {
                accu.append(",");
            }
            accu.append(flagCoding.getSampleName(i));
        }
        return accu.toString();
    }

    private static String zarrTypeOf(int dataType) {
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
}
